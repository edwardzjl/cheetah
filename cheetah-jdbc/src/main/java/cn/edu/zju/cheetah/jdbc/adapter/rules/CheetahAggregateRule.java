package cn.edu.zju.cheetah.jdbc.adapter.rules;

import cn.edu.zju.cheetah.jdbc.adapter.CheetahQuery;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Util;

/**
 * Rule to push an {@link org.apache.calcite.rel.core.Aggregate} into a {@link CheetahQuery}.
 */
class CheetahAggregateRule extends RelOptRule {

    private static final CheetahAggregateRule instance;
    static {
        instance = new CheetahAggregateRule();
    }

    static CheetahAggregateRule getInstance() {
        return instance;
    }

    private CheetahAggregateRule() {
        super(operand(Aggregate.class, operand(CheetahQuery.class, none())));
    }

    public void onMatch(RelOptRuleCall call) {
        final Aggregate aggregate = call.rel(0);
        final CheetahQuery query = call.rel(1);
        if (!CheetahQuery.isValidSignature(query.signature() + 'a')) {
            return;
        }
        if (aggregate.indicator
                || aggregate.getGroupSets().size() != 1
//                || Iterables.any(aggregate.getAggCallList(), CheetahRules.BAD_AGG)
                // edwardlol: change
                || aggregate.getAggCallList().stream().anyMatch(CheetahRules.BAD_AGG)
                || !validAggregate(aggregate, query)) {
            return;
        }
        final RelNode newAggregate = aggregate.copy(aggregate.getTraitSet(),
                ImmutableList.of(Util.last(query.rels)));
        call.transformTo(CheetahQuery.extendQuery(query, newAggregate));
    }

    /* Check whether agg functions reference timestamp */
    private static boolean validAggregate(Aggregate aggregate, CheetahQuery query) {
        ImmutableBitSet.Builder builder = ImmutableBitSet.builder();
        for (AggregateCall aggCall : aggregate.getAggCallList()) {
            builder.addAll(aggCall.getArgList());
        }
        return !CheetahRules.checkTimestampRefOnQuery(builder.build(), query.getTopNode(), query);
    }
}
