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

package com.starrocks.planner;

import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.sql.plan.PlanTestBase;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * FE optimizer tests for S2 spatial index rewrite rule ({@code RewriteToSpatialPlanRule}).
 *
 * <p>Validates that spatial predicates (e.g., {@code st_contains}) on columns with an S2 index
 * trigger the S2INDEX plan annotation, and that non-matching patterns correctly show S2INDEX: OFF.
 */
public class S2IndexTest extends PlanTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        Config.enable_experimental_s2_index = true;
        FeConstants.enablePruneEmptyOutputScan = false;

        // Table with S2 index on geo_col (VARCHAR)
        starRocksAssert.withTable("CREATE TABLE test.test_s2_geo ("
                + " id INT,"
                + " geo_col VARCHAR(65533),"
                + " other_col VARCHAR(256),"
                + " INDEX idx_s2 (geo_col) USING S2 ('s2_level' = '13') "
                + ") "
                + "DUPLICATE KEY(id) "
                + "DISTRIBUTED BY HASH(id) BUCKETS 1 "
                + "PROPERTIES ('replication_num'='1');");

        // Table without S2 index
        starRocksAssert.withTable("CREATE TABLE test.test_no_s2_index ("
                + " id INT,"
                + " geo_col VARCHAR(65533)"
                + ") "
                + "DUPLICATE KEY(id) "
                + "DISTRIBUTED BY HASH(id) BUCKETS 1 "
                + "PROPERTIES ('replication_num'='1');");

        // Table with S2 index at a different level
        starRocksAssert.withTable("CREATE TABLE test.test_s2_level20 ("
                + " id INT,"
                + " location VARCHAR(65533),"
                + " INDEX idx_s2_loc (location) USING S2 ('s2_level' = '20') "
                + ") "
                + "DUPLICATE KEY(id) "
                + "DISTRIBUTED BY HASH(id) BUCKETS 1 "
                + "PROPERTIES ('replication_num'='1');");
    }

    // ==================== Positive cases: S2INDEX should be ON ====================

    @Test
    public void testBasicStContainsWithStringLiteral() throws Exception {
        // st_contains(geo_column, 'POINT(...)') — column first, constant WKT second
        String sql = "select id, geo_col from test_s2_geo "
                + "where st_contains(geo_col, 'POINT(1.0 2.0)')";
        String plan = getVerboseExplain(sql);
        assertContains(plan, "S2INDEX: ON");
        assertContains(plan, "Predicate: st_contains");
        assertContains(plan, "Query Shape: POINT(1.0 2.0)");
        // S2 is a pre-filter — the predicate must still be present in the plan
        assertContains(plan, "st_contains");
    }

    @Test
    public void testStContainsReversedArgumentOrder() throws Exception {
        // st_contains('POLYGON(...)', geo_column) — constant WKT first, column second
        String sql = "select id from test_s2_geo "
                + "where st_contains('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))', geo_col)";
        String plan = getVerboseExplain(sql);
        assertContains(plan, "S2INDEX: ON");
        assertContains(plan, "Predicate: st_contains");
        assertContains(plan, "Query Shape: POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))");
    }

    @Test
    public void testStContainsWithStPoint() throws Exception {
        // st_contains(geo_col, ST_Point(1.5, 2.5)) — ST_Point constructor
        String sql = "select id from test_s2_geo "
                + "where st_contains(geo_col, ST_Point(1.5, 2.5))";
        String plan = getVerboseExplain(sql);
        assertContains(plan, "S2INDEX: ON");
        assertContains(plan, "Query Shape: POINT(1.5 2.5)");
    }

    @Test
    public void testStContainsWithStGeomFromText() throws Exception {
        // st_contains(ST_GeomFromText(geo_col), ST_GeomFromText('POINT(3 4)'))
        String sql = "select id from test_s2_geo "
                + "where st_contains(ST_GeomFromText(geo_col), ST_GeomFromText('POINT(3 4)'))";
        String plan = getVerboseExplain(sql);
        assertContains(plan, "S2INDEX: ON");
        assertContains(plan, "Query Shape: POINT(3 4)");
    }

    @Test
    public void testStContainsInAndPredicate() throws Exception {
        // st_contains in an AND compound predicate — should still match
        String sql = "select id from test_s2_geo "
                + "where id > 0 AND st_contains(geo_col, 'POINT(5 6)')";
        String plan = getVerboseExplain(sql);
        assertContains(plan, "S2INDEX: ON");
        assertContains(plan, "Query Shape: POINT(5 6)");
    }

    @Test
    public void testStContainsWithDifferentTable() throws Exception {
        // Verify S2 index works on a different table with different s2_level
        String sql = "select id from test_s2_level20 "
                + "where st_contains(location, 'POINT(10 20)')";
        String plan = getVerboseExplain(sql);
        assertContains(plan, "S2INDEX: ON");
        assertContains(plan, "Predicate: st_contains");
    }

    // ==================== Negative cases: S2INDEX should be OFF ====================

    @Test
    public void testNoS2IndexOnTable() throws Exception {
        // Table without S2 index — should be OFF
        String sql = "select id from test_no_s2_index "
                + "where st_contains(geo_col, 'POINT(1 2)')";
        String plan = getVerboseExplain(sql);
        assertContains(plan, "S2INDEX: OFF");
    }

    @Test
    public void testWrongColumnNotIndexed() throws Exception {
        // S2 index is on geo_col, but predicate is on other_col — should be OFF
        String sql = "select id from test_s2_geo "
                + "where st_contains(other_col, 'POINT(1 2)')";
        String plan = getVerboseExplain(sql);
        assertContains(plan, "S2INDEX: OFF");
    }

    @Test
    public void testNonSpatialPredicate() throws Exception {
        // No spatial function in predicate — should be OFF
        String sql = "select id from test_s2_geo where id > 10";
        String plan = getVerboseExplain(sql);
        assertContains(plan, "S2INDEX: OFF");
    }

    @Test
    public void testNoPredicateAtAll() throws Exception {
        // No WHERE clause — should be OFF
        String sql = "select id from test_s2_geo";
        String plan = getVerboseExplain(sql);
        assertContains(plan, "S2INDEX: OFF");
    }

    @Test
    public void testBothArgsAreConstants() throws Exception {
        // st_contains('WKT1', 'WKT2') — both constants, no column ref — should be OFF
        String sql = "select id from test_s2_geo "
                + "where st_contains('POLYGON((0 0,1 0,1 1,0 1,0 0))', 'POINT(0.5 0.5)')";
        String plan = getVerboseExplain(sql);
        assertContains(plan, "S2INDEX: OFF");
    }

    @Test
    public void testBothArgsAreColumnRefs() throws Exception {
        // st_contains(geo_col, other_col) — both are column refs, no constant WKT — should be OFF
        String sql = "select id from test_s2_geo "
                + "where st_contains(geo_col, other_col)";
        String plan = getVerboseExplain(sql);
        assertContains(plan, "S2INDEX: OFF");
    }

    @Test
    public void testOrPredicate() throws Exception {
        // OR compound — rule only traverses AND branches
        String sql = "select id from test_s2_geo "
                + "where st_contains(geo_col, 'POINT(1 2)') OR id > 10";
        String plan = getVerboseExplain(sql);
        assertContains(plan, "S2INDEX: OFF");
    }

    @Test
    public void testFeatureFlagDisabled() throws Exception {
        // Temporarily disable the feature flag
        boolean saved = Config.enable_experimental_s2_index;
        try {
            Config.enable_experimental_s2_index = false;
            String sql = "select id from test_s2_geo "
                    + "where st_contains(geo_col, 'POINT(1 2)')";
            String plan = getVerboseExplain(sql);
            // When disabled, the S2INDEX line should not appear at all
            assertNotContains(plan, "S2INDEX:");
        } finally {
            Config.enable_experimental_s2_index = saved;
        }
    }

    // ==================== Predicate preservation tests ====================

    @Test
    public void testPredicatePreservedInPlan() throws Exception {
        // S2 index is a pre-filter; the exact predicate must still appear in the plan
        String sql = "select id from test_s2_geo "
                + "where st_contains(geo_col, 'POINT(7 8)')";
        String plan = getVerboseExplain(sql);
        assertContains(plan, "S2INDEX: ON");
        // The st_contains predicate must still be in the Predicates section (verbose uses lowercase)
        assertContains(plan, "Predicates:");
    }
}
