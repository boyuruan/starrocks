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

#pragma once

#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "common/status.h"
#include "storage/tablet_schema.h"

namespace starrocks {

// S2IndexWriter builds an S2 spatial index for a single VARCHAR/VARBINARY column.
//
// For each row, it parses the WKT (Well-Known Text) geometry string, computes
// S2 cell coverings at the configured level using S2RegionCoverer, and stores
// a mapping from S2 cell IDs to row ordinals.
//
// The resulting .s2i file has the following binary format:
//
//   [Header]
//     magic:        uint32_t  = 0x53324958 ("S2IX")
//     version:      uint32_t  = 1
//     s2_level:     uint32_t
//     num_rows:     uint32_t
//     num_entries:  uint32_t  (number of distinct cell_id entries)
//
//   [Cell Entries] (repeated num_entries times, sorted by cell_id)
//     cell_id:      uint64_t  (S2CellId value)
//     num_row_ids:  uint32_t
//     row_ids:      uint32_t[] (num_row_ids values, sorted)
//
//   [Footer]
//     checksum:     uint32_t  (CRC32 of all preceding bytes)
//
// Integration: Created in ScalarColumnWriter::init() when need_s2_index is true.
// Data is fed via add_values()/add_nulls() calls (same pattern as InvertedWriter).
// The index file is written during write_s2_index().

class S2IndexWriter {
public:
    S2IndexWriter(std::shared_ptr<TabletIndex> tablet_index, std::string s2_index_file_path);

    ~S2IndexWriter() = default;

    // Initialize the writer. Reads s2_level from tablet index properties.
    Status init();

    // Add values from a batch of Slice pointers. Each Slice contains a WKT string.
    // The data pointer points to an array of Slice structs.
    void add_values(const void* values, size_t count);

    // Add null rows (they are skipped in the index).
    void add_nulls(uint32_t count);

    // Write the .s2i index file and report its size.
    Status finish(uint64_t* index_size);

    // Current memory usage estimate.
    uint64_t size() const;

    static constexpr uint32_t S2_INDEX_MAGIC = 0x53324958; // "S2IX"
    static constexpr uint32_t S2_INDEX_VERSION = 1;
    static constexpr int DEFAULT_S2_LEVEL = 13;

private:
    std::shared_ptr<TabletIndex> _tablet_index;
    std::string _s2_index_file_path;
    int _s2_level = DEFAULT_S2_LEVEL;

    // Mapping from S2 cell ID to sorted list of row ordinals.
    std::map<uint64_t, std::vector<uint32_t>> _cell_to_rows;

    // Current row ordinal (incremented by add_values and add_nulls).
    uint32_t _next_row_id = 0;

    // Number of rows with valid geometries indexed.
    uint32_t _num_indexed_rows = 0;

    // Approximate memory usage.
    uint64_t _mem_usage = 0;
};

} // namespace starrocks
