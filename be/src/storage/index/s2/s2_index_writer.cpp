// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "storage/index/s2/s2_index_writer.h"

#include <s2/s2cell_id.h>
#include <s2/s2latlng.h>
#include <s2/s2point_region.h>
#include <s2/s2polygon.h>
#include <s2/s2polyline.h>
#include <s2/s2region_coverer.h>

#include <algorithm>

#include "base/string/slice.h"
#include "fs/fs.h"
#include "fs/fs_util.h"
#include "geo/geo_types.h"
#include "util/crc32c.h"

namespace starrocks {

S2IndexWriter::S2IndexWriter(std::shared_ptr<TabletIndex> tablet_index, std::string s2_index_file_path)
        : _tablet_index(std::move(tablet_index)), _s2_index_file_path(std::move(s2_index_file_path)) {}

Status S2IndexWriter::init() {
    // Read s2_level from tablet index properties.
    const auto& properties = _tablet_index->index_properties();
    auto it = properties.find("s2_level");
    if (it != properties.end()) {
        _s2_level = std::stoi(it->second);
        if (_s2_level < 0 || _s2_level > 30) {
            return Status::InvalidArgument(fmt::format("s2_level must be in [0, 30], got {}", _s2_level));
        }
    }
    return Status::OK();
}

void S2IndexWriter::add_values(const void* values, size_t count) {
    const auto* slices = reinterpret_cast<const Slice*>(values);

    S2RegionCoverer::Options options;
    options.set_fixed_level(_s2_level);

    for (size_t i = 0; i < count; i++) {
        const Slice& wkt = slices[i];
        uint32_t row_id = _next_row_id++;

        // Parse WKT string into a GeoShape.
        GeoParseStatus parse_status;
        std::unique_ptr<GeoShape> shape(GeoShape::from_wkt(wkt.data, wkt.size, &parse_status));
        if (shape == nullptr || parse_status != GEO_PARSE_OK) {
            // Skip rows with invalid/unparseable WKT — they won't match any spatial query.
            continue;
        }

        // Compute S2 cell covering based on geometry type.
        std::vector<S2CellId> covering;
        S2RegionCoverer coverer(options);

        switch (shape->type()) {
        case GEO_SHAPE_POINT: {
            auto* point = static_cast<GeoPoint*>(shape.get());
            S2CellId cell_id(*(point->point()));
            // For a point, the covering at the fixed level is just one cell.
            covering.push_back(cell_id.parent(_s2_level));
            break;
        }
        case GEO_SHAPE_LINE_STRING: {
            auto* line = static_cast<GeoLine*>(shape.get());
            const S2Polyline* polyline = line->polyline();
            if (polyline != nullptr) {
                coverer.GetCovering(*polyline, &covering);
            }
            break;
        }
        case GEO_SHAPE_POLYGON: {
            auto* poly = static_cast<GeoPolygon*>(shape.get());
            const S2Polygon* polygon = poly->polygon();
            if (polygon != nullptr) {
                coverer.GetCovering(*polygon, &covering);
            }
            break;
        }
        default:
            // Unsupported geometry type — skip.
            continue;
        }

        // Add this row to all covering cells.
        for (const auto& cell_id : covering) {
            auto& row_ids = _cell_to_rows[cell_id.id()];
            row_ids.push_back(row_id);
            _mem_usage += sizeof(uint32_t); // approximate
        }
        if (_cell_to_rows.size() * sizeof(uint64_t) > _mem_usage) {
            _mem_usage = _cell_to_rows.size() * (sizeof(uint64_t) + sizeof(std::vector<uint32_t>));
        }
        _num_indexed_rows++;
    }
}

void S2IndexWriter::add_nulls(uint32_t count) {
    _next_row_id += count;
}

Status S2IndexWriter::finish(uint64_t* index_size) {
    if (_next_row_id == 0) {
        // No rows written — write an empty marker file.
        ASSIGN_OR_RETURN(auto wfile, fs::new_writable_file(_s2_index_file_path));
        // Write header with zero entries.
        uint32_t magic = S2_INDEX_MAGIC;
        uint32_t version = S2_INDEX_VERSION;
        uint32_t s2_level = static_cast<uint32_t>(_s2_level);
        uint32_t num_rows = 0;
        uint32_t num_entries = 0;

        RETURN_IF_ERROR(wfile->append(Slice(reinterpret_cast<const char*>(&magic), sizeof(magic))));
        RETURN_IF_ERROR(wfile->append(Slice(reinterpret_cast<const char*>(&version), sizeof(version))));
        RETURN_IF_ERROR(wfile->append(Slice(reinterpret_cast<const char*>(&s2_level), sizeof(s2_level))));
        RETURN_IF_ERROR(wfile->append(Slice(reinterpret_cast<const char*>(&num_rows), sizeof(num_rows))));
        RETURN_IF_ERROR(wfile->append(Slice(reinterpret_cast<const char*>(&num_entries), sizeof(num_entries))));

        // CRC32 of header.
        uint32_t crc = 0;
        crc = crc32c::Extend(crc, reinterpret_cast<const char*>(&magic), sizeof(magic));
        crc = crc32c::Extend(crc, reinterpret_cast<const char*>(&version), sizeof(version));
        crc = crc32c::Extend(crc, reinterpret_cast<const char*>(&s2_level), sizeof(s2_level));
        crc = crc32c::Extend(crc, reinterpret_cast<const char*>(&num_rows), sizeof(num_rows));
        crc = crc32c::Extend(crc, reinterpret_cast<const char*>(&num_entries), sizeof(num_entries));
        RETURN_IF_ERROR(wfile->append(Slice(reinterpret_cast<const char*>(&crc), sizeof(crc))));

        RETURN_IF_ERROR(wfile->close());
        if (index_size) {
            ASSIGN_OR_RETURN(auto file_ptr, fs::new_random_access_file(_s2_index_file_path));
            ASSIGN_OR_RETURN(auto file_size, file_ptr->get_size());
            *index_size += file_size;
        }
        return Status::OK();
    }

    // Sort row_ids within each cell entry and write the index file.
    ASSIGN_OR_RETURN(auto wfile, fs::new_writable_file(_s2_index_file_path));

    uint32_t crc = 0;

    // Write header.
    uint32_t magic = S2_INDEX_MAGIC;
    uint32_t version = S2_INDEX_VERSION;
    uint32_t s2_level = static_cast<uint32_t>(_s2_level);
    uint32_t num_rows = _next_row_id;
    uint32_t num_entries = static_cast<uint32_t>(_cell_to_rows.size());

    auto append_and_crc = [&wfile, &crc](const void* data, size_t len) -> Status {
        RETURN_IF_ERROR(wfile->append(Slice(reinterpret_cast<const char*>(data), len)));
        crc = crc32c::Extend(crc, reinterpret_cast<const char*>(data), len);
        return Status::OK();
    };

    RETURN_IF_ERROR(append_and_crc(&magic, sizeof(magic)));
    RETURN_IF_ERROR(append_and_crc(&version, sizeof(version)));
    RETURN_IF_ERROR(append_and_crc(&s2_level, sizeof(s2_level)));
    RETURN_IF_ERROR(append_and_crc(&num_rows, sizeof(num_rows)));
    RETURN_IF_ERROR(append_and_crc(&num_entries, sizeof(num_entries)));

    // Write cell entries (already sorted by cell_id since we use std::map).
    for (auto& [cell_id, row_ids] : _cell_to_rows) {
        // Sort and deduplicate row IDs.
        std::sort(row_ids.begin(), row_ids.end());
        row_ids.erase(std::unique(row_ids.begin(), row_ids.end()), row_ids.end());

        uint32_t num_row_ids = static_cast<uint32_t>(row_ids.size());

        RETURN_IF_ERROR(append_and_crc(&cell_id, sizeof(cell_id)));
        RETURN_IF_ERROR(append_and_crc(&num_row_ids, sizeof(num_row_ids)));
        RETURN_IF_ERROR(append_and_crc(row_ids.data(), num_row_ids * sizeof(uint32_t)));
    }

    // Write footer checksum.
    RETURN_IF_ERROR(wfile->append(Slice(reinterpret_cast<const char*>(&crc), sizeof(crc))));
    RETURN_IF_ERROR(wfile->close());

    if (index_size) {
        ASSIGN_OR_RETURN(auto file_ptr, fs::new_random_access_file(_s2_index_file_path));
        ASSIGN_OR_RETURN(auto file_size, file_ptr->get_size());
        *index_size += file_size;
    }

    // Free memory.
    _cell_to_rows.clear();
    _mem_usage = 0;

    return Status::OK();
}

uint64_t S2IndexWriter::size() const {
    return _mem_usage;
}

} // namespace starrocks
