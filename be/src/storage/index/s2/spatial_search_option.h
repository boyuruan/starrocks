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
#include <memory>
#include <string>

namespace starrocks {

// Options for spatial search using the S2 index, populated from TSpatialSearchOptions
// in the query plan. Passed through TabletReaderParams → SegmentReadOptions →
// SegmentIterator.
struct SpatialSearchOption {
    // Whether to use the S2 index for spatial pre-filtering
    bool enable_use_s2_index = false;

    // WKT representation of the query shape (e.g., "POINT(1 2)", "POLYGON(...)")
    std::string query_shape_wkt;

    // Spatial predicate type: "st_contains", "st_intersects", "st_within", etc.
    std::string spatial_predicate;

    // Optional distance threshold for proximity queries (meters). Negative means unused.
    double distance_threshold = -1.0;

    // Column unique ID of the spatial column that has the S2 index
    int32_t spatial_column_id = -1;

    // Override for S2 level (if different from the index's stored level). -1 means use index default.
    int32_t s2_level_override = -1;

    SpatialSearchOption() = default;
};

using SpatialSearchOptionPtr = std::shared_ptr<SpatialSearchOption>;

} // namespace starrocks
