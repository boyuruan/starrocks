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

import com.starrocks.thrift.TSpatialSearchOptions;

/**
 * Java POJO that mirrors TSpatialSearchOptions.
 * Carries spatial query parameters from the optimizer to the BE scan node.
 */
public class SpatialSearchOptions {
    private boolean enableUseS2Index = false;
    private String queryShapeWkt = "";
    private String spatialPredicate = "";
    private double distanceThreshold = -1;
    private int spatialColumnId = -1;
    private int s2LevelOverride = -1;

    public boolean isEnableUseS2Index() {
        return enableUseS2Index;
    }

    public void setEnableUseS2Index(boolean enableUseS2Index) {
        this.enableUseS2Index = enableUseS2Index;
    }

    public String getQueryShapeWkt() {
        return queryShapeWkt;
    }

    public void setQueryShapeWkt(String queryShapeWkt) {
        this.queryShapeWkt = queryShapeWkt;
    }

    public String getSpatialPredicate() {
        return spatialPredicate;
    }

    public void setSpatialPredicate(String spatialPredicate) {
        this.spatialPredicate = spatialPredicate;
    }

    public double getDistanceThreshold() {
        return distanceThreshold;
    }

    public void setDistanceThreshold(double distanceThreshold) {
        this.distanceThreshold = distanceThreshold;
    }

    public int getSpatialColumnId() {
        return spatialColumnId;
    }

    public void setSpatialColumnId(int spatialColumnId) {
        this.spatialColumnId = spatialColumnId;
    }

    public int getS2LevelOverride() {
        return s2LevelOverride;
    }

    public void setS2LevelOverride(int s2LevelOverride) {
        this.s2LevelOverride = s2LevelOverride;
    }

    public TSpatialSearchOptions toThrift() {
        TSpatialSearchOptions opts = new TSpatialSearchOptions();
        opts.setEnable_use_s2_index(true);
        opts.setQuery_shape_wkt(queryShapeWkt);
        opts.setSpatial_predicate(spatialPredicate);
        opts.setSpatial_column_id(spatialColumnId);
        if (distanceThreshold >= 0) {
            opts.setDistance_threshold(distanceThreshold);
        }
        if (s2LevelOverride >= 0) {
            opts.setS2_level_override(s2LevelOverride);
        }
        return opts;
    }

    public String getExplainString(String prefix) {
        return prefix + "S2INDEX: ON" + "\n" +
                prefix + prefix +
                "Predicate: " + spatialPredicate + ", " +
                "Query Shape: " + queryShapeWkt + ", " +
                "Column ID: " + spatialColumnId +
                (distanceThreshold >= 0 ? ", Distance: " + distanceThreshold : "") +
                (s2LevelOverride >= 0 ? ", Level Override: " + s2LevelOverride : "") +
                "\n";
    }
}
