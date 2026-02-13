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

package com.starrocks.common;

import com.starrocks.common.io.ParamsKey;
import com.starrocks.sql.analyzer.SemanticException;

import java.util.Arrays;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Parameters for S2 index used in geo-related functions.
 * S2 index utilizes Google's S2 geometry library for efficient spatial indexing.
 */
public class S2IndexParams {

    public static final Set<String> SUPPORTED_PARAM_KEYS = Stream.of(Arrays.stream(CommonIndexParamKey.values()),
                    Arrays.stream(IndexParamsKey.values()), Arrays.stream(SearchParamsKey.values()))
            .flatMap(key -> key.map(k -> k.name().toLowerCase(Locale.ROOT)))
            .collect(Collectors.toSet());

    /**
     * Common parameters for S2 index.
     */
    public enum CommonIndexParamKey implements ParamsKey {
        // S2 cell level for indexing (0-30, default 13)
        // Higher levels provide finer granularity but use more storage
        S2_LEVEL {
            @Override
            public void check(String value) {
                validateInteger(value, "S2_LEVEL", 0, 30);
            }
        }
    }

    /**
     * Index build parameters for S2 index.
     */
    public enum IndexParamsKey implements ParamsKey {
        // No specific index build parameters for now
        // Future parameters could include:
        // - min_level/max_level for adaptive cell sizing
        // - max_cells for covering configuration
    }

    /**
     * Search parameters for S2 index.
     */
    public enum SearchParamsKey implements ParamsKey {
        // No specific search parameters for now
        // Future parameters could include:
        // - covering_level for query region covering
    }

    private static void validateInteger(String value, String paramName, int minValue, int maxValue) {
        try {
            int intValue = Integer.parseInt(value);
            if (intValue < minValue || intValue > maxValue) {
                throw new SemanticException(
                        String.format("Value of `%s` must be between %d and %d", paramName, minValue, maxValue));
            }
        } catch (NumberFormatException e) {
            throw new SemanticException(String.format("Value of `%s` must be a valid integer", paramName));
        }
    }
}
