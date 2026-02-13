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

#include "storage/index/s2/s2_index_reader.h"

#include <algorithm>
#include <cstring>

#include "fs/fs.h"
#include "util/crc32c.h"

namespace starrocks {

Status S2IndexReader::open(const std::string& index_file_path) {
    ASSIGN_OR_RETURN(auto rfile, fs::new_random_access_file(index_file_path));
    ASSIGN_OR_RETURN(auto file_size, rfile->get_size());

    if (file_size < 24) { // 5 * uint32_t header + uint32_t footer checksum
        return Status::Corruption(
                fmt::format("S2 index file too small: {} bytes, path: {}", file_size, index_file_path));
    }

    // Read entire file into memory.
    std::string buffer(file_size, '\0');
    RETURN_IF_ERROR(rfile->read_fully(buffer.data(), file_size));

    const char* data = buffer.data();
    size_t offset = 0;

    // Parse header.
    uint32_t magic = 0;
    std::memcpy(&magic, data + offset, sizeof(magic));
    offset += sizeof(magic);

    if (magic != S2IndexWriter::S2_INDEX_MAGIC) {
        return Status::Corruption(
                fmt::format("Invalid S2 index magic: 0x{:08X}, expected 0x{:08X}, path: {}", magic,
                            S2IndexWriter::S2_INDEX_MAGIC, index_file_path));
    }

    uint32_t version = 0;
    std::memcpy(&version, data + offset, sizeof(version));
    offset += sizeof(version);

    if (version != S2IndexWriter::S2_INDEX_VERSION) {
        return Status::Corruption(
                fmt::format("Unsupported S2 index version: {}, expected {}, path: {}", version,
                            S2IndexWriter::S2_INDEX_VERSION, index_file_path));
    }

    uint32_t s2_level = 0;
    std::memcpy(&s2_level, data + offset, sizeof(s2_level));
    offset += sizeof(s2_level);
    _s2_level = static_cast<int>(s2_level);

    std::memcpy(&_num_rows, data + offset, sizeof(_num_rows));
    offset += sizeof(_num_rows);

    std::memcpy(&_num_entries, data + offset, sizeof(_num_entries));
    offset += sizeof(_num_entries);

    // Parse cell entries.
    for (uint32_t i = 0; i < _num_entries; i++) {
        if (offset + sizeof(uint64_t) + sizeof(uint32_t) > file_size - sizeof(uint32_t)) {
            return Status::Corruption(
                    fmt::format("S2 index file truncated at entry {}/{}, path: {}", i, _num_entries, index_file_path));
        }

        uint64_t cell_id = 0;
        std::memcpy(&cell_id, data + offset, sizeof(cell_id));
        offset += sizeof(cell_id);

        uint32_t num_row_ids = 0;
        std::memcpy(&num_row_ids, data + offset, sizeof(num_row_ids));
        offset += sizeof(num_row_ids);

        if (offset + num_row_ids * sizeof(uint32_t) > file_size - sizeof(uint32_t)) {
            return Status::Corruption(fmt::format(
                    "S2 index file truncated at entry {}/{} row_ids, path: {}", i, _num_entries, index_file_path));
        }

        std::vector<uint32_t> row_ids(num_row_ids);
        std::memcpy(row_ids.data(), data + offset, num_row_ids * sizeof(uint32_t));
        offset += num_row_ids * sizeof(uint32_t);

        _cell_to_rows[cell_id] = std::move(row_ids);
    }

    // Validate checksum.
    uint32_t stored_crc = 0;
    std::memcpy(&stored_crc, data + offset, sizeof(stored_crc));

    uint32_t computed_crc = crc32c::Value(data, offset);
    if (stored_crc != computed_crc) {
        return Status::Corruption(fmt::format("S2 index checksum mismatch: stored 0x{:08X} vs computed 0x{:08X}, "
                                              "path: {}",
                                              stored_crc, computed_crc, index_file_path));
    }

    _is_open = true;
    return Status::OK();
}

Status S2IndexReader::query_cells(const std::vector<uint64_t>& cell_ids, std::vector<uint32_t>* row_ids) const {
    if (!_is_open) {
        return Status::InternalError("S2IndexReader not opened");
    }

    row_ids->clear();

    for (const auto& cell_id : cell_ids) {
        auto it = _cell_to_rows.find(cell_id);
        if (it != _cell_to_rows.end()) {
            row_ids->insert(row_ids->end(), it->second.begin(), it->second.end());
        }
    }

    // Sort and deduplicate.
    std::sort(row_ids->begin(), row_ids->end());
    row_ids->erase(std::unique(row_ids->begin(), row_ids->end()), row_ids->end());

    return Status::OK();
}

} // namespace starrocks
