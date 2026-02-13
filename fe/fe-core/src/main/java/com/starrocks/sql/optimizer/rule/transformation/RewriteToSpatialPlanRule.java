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
package com.starrocks.sql.optimizer.rule.transformation;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.ColumnId;
import com.starrocks.catalog.Index;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.Config;
import com.starrocks.common.SpatialSearchOptions;
import com.starrocks.sql.ast.IndexDef;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.starrocks.catalog.FunctionSet.ST_CONTAINS;

/**
 * Optimizer rule that detects spatial predicates (st_contains) in OlapScan predicates
 * and populates SpatialSearchOptions to enable S2 index filtering at the BE scan level.
 *
 * <p>Unlike the vector index rule which removes predicates (because the vector index produces
 * exact results), this rule keeps the spatial predicates in place. The S2 index is a
 * candidate/pre-filter — it narrows down rows using S2 cell coverings, and the exact
 * spatial predicate still evaluates on surviving rows.
 *
 * <p>Supported predicate patterns:
 * <ul>
 *   <li>{@code st_contains(geo_column, 'POINT(...)')} — column vs constant WKT</li>
 *   <li>{@code st_contains('POLYGON(...)', geo_column)} — constant WKT vs column</li>
 *   <li>{@code st_contains(ST_GeomFromText(geo_column), ST_GeomFromText('...'))} — wrapped forms</li>
 * </ul>
 *
 * <p>The S2-indexed column stores WKT strings as VARCHAR. The {@code st_contains} function
 * has signature {@code st_contains(VARCHAR, VARCHAR) -> BOOLEAN}. StarRocks does not have
 * a dedicated GEOMETRY type; all geometry data flows through VARCHAR.
 *
 * <p>Note: {@code st_distance_sphere} is not yet supported because it takes 4 DOUBLE
 * coordinate arguments directly, not geometry column references.
 */
public class RewriteToSpatialPlanRule extends TransformationRule {

    /**
     * Set of geo constructor function names that wrap a column or constant into a geometry.
     * When we encounter {@code ST_GeomFromText(column)}, we unwrap to get the underlying column ref.
     * When we encounter {@code ST_GeomFromText('WKT')}, we extract the WKT string.
     */
    private static final Set<String> GEO_CONSTRUCTOR_FUNCTIONS = Set.of(
            "st_geometryfromtext", "st_geomfromtext",
            "st_polyfromtext", "st_polygonfromtext",
            "st_linefromtext", "st_linestringfromtext"
    );

    public RewriteToSpatialPlanRule() {
        super(RuleType.TF_SPATIAL_REWRITE_RULE,
                Pattern.create(OperatorType.LOGICAL_OLAP_SCAN));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        if (!Config.enable_experimental_s2_index) {
            return false;
        }

        LogicalOlapScanOperator scanOp = (LogicalOlapScanOperator) input.getOp();
        if (scanOp.getPredicate() == null) {
            return false;
        }

        // Don't rewrite if spatial search is already enabled (idempotency)
        if (scanOp.getSpatialSearchOptions().isEnableUseS2Index()) {
            return false;
        }

        // Table must have an S2 index
        OlapTable table = (OlapTable) scanOp.getTable();
        return table.getIndexes().stream()
                .anyMatch(i -> i.getIndexType() == IndexDef.IndexType.S2);
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalOlapScanOperator scanOp = (LogicalOlapScanOperator) input.getOp();
        ScalarOperator predicate = scanOp.getPredicate();

        Optional<SpatialFuncInfo> optionalInfo = extractSpatialFuncInfo(predicate, scanOp);
        if (optionalInfo.isEmpty()) {
            return List.of();
        }

        SpatialFuncInfo info = optionalInfo.get();
        SpatialSearchOptions opts = scanOp.getSpatialSearchOptions();
        opts.setEnableUseS2Index(true);
        opts.setSpatialPredicate(info.functionName);
        opts.setQueryShapeWkt(info.queryShapeWkt);
        opts.setSpatialColumnId(info.columnUniqueId);

        // Keep predicate intact — S2 index is a pre-filter, not an exact filter
        LogicalOlapScanOperator newScanOp = LogicalOlapScanOperator.builder()
                .withOperator(scanOp)
                .build();

        return List.of(OptExpression.create(newScanOp));
    }

    /**
     * Search the predicate tree for a spatial function call that can leverage an S2 index.
     * Handles both direct predicates and AND-compound predicates (picks the first match).
     */
    private Optional<SpatialFuncInfo> extractSpatialFuncInfo(ScalarOperator predicate,
                                                              LogicalOlapScanOperator scanOp) {
        if (predicate instanceof CallOperator) {
            return matchSpatialCall((CallOperator) predicate, scanOp);
        }

        if (predicate instanceof CompoundPredicateOperator) {
            CompoundPredicateOperator compound = (CompoundPredicateOperator) predicate;
            if (compound.isAnd()) {
                for (ScalarOperator child : predicate.getChildren()) {
                    Optional<SpatialFuncInfo> result = extractSpatialFuncInfo(child, scanOp);
                    if (result.isPresent()) {
                        return result;
                    }
                }
            }
        }

        return Optional.empty();
    }

    /**
     * Check if a CallOperator is st_contains with one column-ref argument
     * (matching an S2 index column) and one constant WKT argument.
     *
     * <p>Handles direct column references and column references wrapped in
     * geo constructor functions like ST_GeomFromText(column).
     */
    private Optional<SpatialFuncInfo> matchSpatialCall(CallOperator call, LogicalOlapScanOperator scanOp) {
        String fnName = call.getFnName();
        if (!ST_CONTAINS.equalsIgnoreCase(fnName)) {
            return Optional.empty();
        }

        if (call.getChildren().size() != 2) {
            return Optional.empty();
        }

        ScalarOperator arg0 = call.getChild(0);
        ScalarOperator arg1 = call.getChild(1);

        // Try both argument orders: fn(column, constant) and fn(constant, column)
        ColumnRefOperator colRef = null;
        String wkt = null;

        ColumnRefOperator colRef0 = extractColumnRef(arg0);
        ColumnRefOperator colRef1 = extractColumnRef(arg1);

        if (colRef0 != null && isConstantWkt(arg1)) {
            colRef = colRef0;
            wkt = extractWkt(arg1);
        } else if (colRef1 != null && isConstantWkt(arg0)) {
            colRef = colRef1;
            wkt = extractWkt(arg0);
        }

        if (colRef == null || wkt == null) {
            return Optional.empty();
        }

        // Match column against S2 index columns
        OlapTable table = (OlapTable) scanOp.getTable();
        Column column = scanOp.getColRefToColumnMetaMap().get(colRef);
        if (column == null) {
            return Optional.empty();
        }

        Index s2Index = findMatchingS2Index(table, column);
        if (s2Index == null) {
            return Optional.empty();
        }

        int columnUniqueId = column.getUniqueId();

        return Optional.of(new SpatialFuncInfo(fnName, wkt, columnUniqueId));
    }

    /**
     * Extract a ColumnRefOperator from an expression, unwrapping geo constructor
     * functions if necessary.
     *
     * <p>Handles:
     * <ul>
     *   <li>Direct column ref: {@code column}</li>
     *   <li>Wrapped column ref: {@code ST_GeomFromText(column)}</li>
     * </ul>
     *
     * @return the underlying ColumnRefOperator, or null if not found
     */
    private ColumnRefOperator extractColumnRef(ScalarOperator operator) {
        if (operator instanceof ColumnRefOperator) {
            return (ColumnRefOperator) operator;
        }
        // Unwrap geo constructor: ST_GeomFromText(column) -> column
        if (operator instanceof CallOperator) {
            CallOperator call = (CallOperator) operator;
            if (GEO_CONSTRUCTOR_FUNCTIONS.contains(call.getFnName().toLowerCase()) &&
                    call.getChildren().size() == 1 &&
                    call.getChild(0) instanceof ColumnRefOperator) {
                return (ColumnRefOperator) call.getChild(0);
            }
        }
        return null;
    }

    /**
     * Find an S2 index on the table that covers the given column.
     */
    private Index findMatchingS2Index(OlapTable table, Column column) {
        ColumnId columnId = column.getColumnId();
        for (Index index : table.getIndexes()) {
            if (index.getIndexType() == IndexDef.IndexType.S2) {
                if (!index.getColumns().isEmpty() && index.getColumns().get(0).equals(columnId)) {
                    return index;
                }
            }
        }
        return null;
    }

    /**
     * Check if an operator represents a constant WKT value.
     * Supports:
     * <ul>
     *   <li>String constants (e.g., {@code 'POINT(1 2)'})</li>
     *   <li>Geo constructor calls with all-constant args (e.g., {@code ST_Point(1.0, 2.0)},
     *       {@code ST_GeomFromText('POLYGON(...)')})</li>
     * </ul>
     */
    private boolean isConstantWkt(ScalarOperator operator) {
        if (operator instanceof ConstantOperator) {
            return operator.getType().isStringType();
        }
        // ST_Point(lng, lat), ST_GeomFromText('...'), etc. with constant args
        if (operator instanceof CallOperator) {
            CallOperator call = (CallOperator) operator;
            return call.getChildren().stream().allMatch(ScalarOperator::isConstant);
        }
        return false;
    }

    /**
     * Extract WKT string from a constant operator or a spatial constructor function.
     *
     * @return WKT string, or null if extraction fails
     */
    private String extractWkt(ScalarOperator operator) {
        if (operator instanceof ConstantOperator) {
            ConstantOperator constant = (ConstantOperator) operator;
            Object value = constant.getValue();
            return value != null ? value.toString() : null;
        }
        if (operator instanceof CallOperator) {
            CallOperator call = (CallOperator) operator;
            String callFnName = call.getFnName().toLowerCase();

            // For ST_Point(lng, lat), construct WKT
            if ("st_point".equals(callFnName) && call.getChildren().size() == 2) {
                ScalarOperator lngOp = call.getChild(0);
                ScalarOperator latOp = call.getChild(1);
                if (lngOp instanceof ConstantOperator && latOp instanceof ConstantOperator) {
                    return "POINT(" + ((ConstantOperator) lngOp).getValue() + " " +
                            ((ConstantOperator) latOp).getValue() + ")";
                }
            }
            // For ST_GeomFromText('WKT') and similar, extract the WKT argument
            if (GEO_CONSTRUCTOR_FUNCTIONS.contains(callFnName) && !call.getChildren().isEmpty()) {
                ScalarOperator wktArg = call.getChild(0);
                if (wktArg instanceof ConstantOperator) {
                    Object value = ((ConstantOperator) wktArg).getValue();
                    return value != null ? value.toString() : null;
                }
            }
        }
        return null;
    }

    /**
     * Internal holder for matched spatial function information.
     */
    private static class SpatialFuncInfo {
        final String functionName;
        final String queryShapeWkt;
        final int columnUniqueId;

        SpatialFuncInfo(String functionName, String queryShapeWkt, int columnUniqueId) {
            this.functionName = functionName;
            this.queryShapeWkt = queryShapeWkt;
            this.columnUniqueId = columnUniqueId;
        }
    }
}
