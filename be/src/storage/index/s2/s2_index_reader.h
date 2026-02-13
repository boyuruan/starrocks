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
#include <string>
#include <vector>

#include "common/status.h"
#include "storage/index/s2/s2_index_writer.h"

namespace starrocks {

// S2IndexReader reads a .s2i file produced by S2IndexWriter and provides
// spatial lookup capabilities.
//
// Usage:
//   1. Create an S2IndexReader
//   2. Call open() to read and validate the index file
//   3. Call query_cells() with a set of S2 cell IDs to get candidate row IDs
//
// The reader loads the entire index into memory (the index is expected to be
// small relative to the data). For a segment with N rows and M covering cells
// on average, the index has at most N*M cell-to-row entries.
//
// Thread safety: After open() completes, query_cells() is safe to call from
// multiple threads concurrently (read-only access to immutable data).

class S2IndexReader {
public:
    S2IndexReader() = default;
    ~S2IndexReader() = default;

    // Open and read the .s2i file. Validates magic, version, and checksum.
    Status open(const std::string& index_file_path);

    // Given a set of S2 cell IDs (at the index's configured level), return
    // the union of all row IDs that have geometries covering any of those cells.
    // The result is sorted and deduplicated.
    Status query_cells(const std::vector<uint64_t>& cell_ids, std::vector<uint32_t>* row_ids) const;

    // Return the S2 level used by this index.
    int s2_level() const { return _s2_level; }

    // Return total number of rows in the indexed segment.
    uint32_t num_rows() const { return _num_rows; }

    // Return number of distinct cell entries in the index.
    uint32_t num_entries() const { return _num_entries; }

    // Return true if the index has been successfully opened.
    bool is_open() const { return _is_open; }

private:
    bool _is_open = false;
    int _s2_level = 0;
    uint32_t _num_rows = 0;
    uint32_t _num_entries = 0;

    // Sorted map from cell_id to sorted row_ids. Loaded from the .s2i file.
    std::map<uint64_t, std::vector<uint32_t>> _cell_to_rows;
};

} // namespace starrocks
