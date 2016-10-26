package cn.edu.zju.cheetah.jdbc.adapter.rules;

import cn.edu.zju.cheetah.jdbc.adapter.CheetahDateTimeUtils;
import cn.edu.zju.cheetah.jdbc.adapter.CheetahQuery;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;
import org.joda.time.Interval;

import java.util.ArrayList;
import java.util.List;

/**
 * Rule to push a {@link org.apache.calcite.rel.core.Filter} into a {@link CheetahQuery}.
 */
class CheetahFilterRule extends RelOptRule {

    private static final CheetahFilterRule instance;
    static {
        instance = new CheetahFilterRule();
    }

    static CheetahFilterRule getInstance() {
        return instance;
    }

    private CheetahFilterRule() {
        super(operand(Filter.class, operand(CheetahQuery.class, none())));
    }

    public void onMatch(RelOptRuleCall call) {
        final Filter filter = call.rel(0);
        final CheetahQuery query = call.rel(1);
        if (!CheetahQuery.isValidSignature(query.signature() + 'f')
                || !query.isValidFilter(filter.getCondition())) {
            return;
        }
        // Timestamp
        int timestampFieldIdx = -1;
        for (int i = 0; i < query.getRowType().getFieldCount(); i++) {
            if (query.cheetahTable.timestampFieldName.equals(
                    query.getRowType().getFieldList().get(i).getName())) {
                timestampFieldIdx = i;
                break;
            }
        }
        final Pair<List<RexNode>, List<RexNode>> pair = splitFilters(
                filter.getCluster().getRexBuilder(), query, filter.getCondition(),
                timestampFieldIdx);
        if (pair == null) {
            // We can't push anything useful to Cheetah.
            return;
        }
        List<Interval> intervals = null;
        if (!pair.left.isEmpty()) {
            intervals = CheetahDateTimeUtils.createInterval(
                    query.getRowType().getFieldList().get(timestampFieldIdx).getType(),
                    RexUtil.composeConjunction(query.getCluster().getRexBuilder(), pair.left, false));
            if (intervals == null) {
                // We can't push anything useful to Cheetah.
                return;
            }
        }
        CheetahQuery newCheetahQuery = query;
        if (!pair.right.isEmpty()) {
            final RelNode newFilter = filter.copy(filter.getTraitSet(), Util.last(query.rels),
                    RexUtil.composeConjunction(filter.getCluster().getRexBuilder(), pair.right, false));
            newCheetahQuery = CheetahQuery.extendQuery(query, newFilter);
        }
        if (intervals != null) {
            newCheetahQuery = CheetahQuery.extendQuery(newCheetahQuery, intervals);
        }
        call.transformTo(newCheetahQuery);
    }

    /* Splits the filter condition in two groups: those that filter on the timestamp column
     * and those that filter on other fields */
    private static Pair<List<RexNode>, List<RexNode>> splitFilters(final RexBuilder rexBuilder,
                                                                   final CheetahQuery input, RexNode cond, final int timestampFieldIdx) {
        final List<RexNode> timeRangeNodes = new ArrayList<>();
        final List<RexNode> otherNodes = new ArrayList<>();
        List<RexNode> conjs = RelOptUtil.conjunctions(cond);
        if (conjs.isEmpty()) {
            // We do not transform
            return null;
        }
        // Number of columns with the dimensions and timestamp
        for (RexNode conj : conjs) {
            final RelOptUtil.InputReferencedVisitor visitor = new RelOptUtil.InputReferencedVisitor();
            conj.accept(visitor);
            if (visitor.inputPosReferenced.contains(timestampFieldIdx)) {
                if (visitor.inputPosReferenced.size() != 1) {
                    // Complex predicate, transformation currently not supported
                    return null;
                }
                timeRangeNodes.add(conj);
            } else {
                for (Integer i : visitor.inputPosReferenced) {
                    if (input.cheetahTable.metricFieldNames.contains(
                            input.getRowType().getFieldList().get(i).getName())) {
                        // Filter on metrics, not supported in Cheetah
                        return null;
                    }
                }
                otherNodes.add(conj);
            }
        }
        return Pair.of(timeRangeNodes, otherNodes);
    }
}
