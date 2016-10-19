package cn.edu.zju.cheetah.jdbc.adapter.rules;

import cn.edu.zju.cheetah.jdbc.adapter.CheetahQuery;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Util;

/**
 * Rule to push an {@link org.apache.calcite.rel.core.Aggregate} into a {@link CheetahQuery}.
 */
public class CheetahSortRule extends RelOptRule {
    CheetahSortRule() {
        super(operand(Sort.class, operand(CheetahQuery.class, none())));
    }

    public void onMatch(RelOptRuleCall call) {
        final Sort sort = call.rel(0);
        final CheetahQuery query = call.rel(1);
        if (!CheetahQuery.isValidSignature(query.signature() + 'l')) {
            return;
        }
        // Either it is:
        // - a sort without limit on the time column on top of
        //     Agg operator (transformable to timeseries query), or
        // - it is a sort w/o limit on columns that do not include
        //     the time column on top of Agg operator, or
        // - a simple limit on top of other operator than Agg
        if (!validSortLimit(sort, query)) {
            return;
        }
        final RelNode newSort = sort.copy(sort.getTraitSet(),
                ImmutableList.of(Util.last(query.rels)));
        call.transformTo(CheetahQuery.extendQuery(query, newSort));
    }

    /* Check sort valid */
    private static boolean validSortLimit(Sort sort, CheetahQuery query) {
        if (sort.offset != null && RexLiteral.intValue(sort.offset) != 0) {
            // offset not supported by Cheetah
            return false;
        }
        if (query.getTopNode() instanceof Aggregate) {
            final Aggregate topAgg = (Aggregate) query.getTopNode();
            final ImmutableBitSet.Builder positionsReferenced = ImmutableBitSet.builder();
            int metricsRefs = 0;
            for (RelFieldCollation col : sort.collation.getFieldCollations()) {
                int idx = col.getFieldIndex();
                if (idx >= topAgg.getGroupCount()) {
                    metricsRefs++;
                    continue;
                }
                positionsReferenced.set(topAgg.getGroupSet().nth(idx));
            }
            boolean refsTimestamp =
                    CheetahRules.checkTimestampRefOnQuery(positionsReferenced.build(), topAgg.getInput(), query);
            if (refsTimestamp && metricsRefs != 0) {
                return false;
            }
            return true;
        }
        // If it is going to be a Cheetah select operator, we push the limit if
        // it does not contain a sort specification (required by Cheetah)
        return RelOptUtil.isPureLimit(sort);
    }
}
