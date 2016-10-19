package cn.edu.zju.cheetah.jdbc.adapter.rules;

import cn.edu.zju.cheetah.jdbc.adapter.CheetahDateTimeUtils;
import cn.edu.zju.cheetah.jdbc.adapter.CheetahQuery;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Util;

import java.util.List;

/**
 * Rule to push an {@link org.apache.calcite.rel.core.Aggregate} and
 * {@link org.apache.calcite.rel.core.Project} into a {@link CheetahQuery}.
 */
public class CheetahProjectAggregateRule extends RelOptRule {
    CheetahProjectAggregateRule() {
        super(operand(Aggregate.class, operand(Project.class, operand(CheetahQuery.class, none()))));
    }

    public void onMatch(RelOptRuleCall call) {
        final Aggregate aggregate = call.rel(0);
        final Project project = call.rel(1);
        final CheetahQuery query = call.rel(2);
        if (!CheetahQuery.isValidSignature(query.signature() + 'p' + 'a')) {
            return;
        }
        int timestampIdx;
        if ((timestampIdx = validProject(project, query)) == -1) {
            return;
        }
        if (aggregate.indicator
                || aggregate.getGroupSets().size() != 1
                || Iterables.any(aggregate.getAggCallList(), CheetahRules.BAD_AGG)
                || !validAggregate(aggregate, timestampIdx)) {
            return;
        }

        final RelNode newProject = project.copy(project.getTraitSet(),
                ImmutableList.of(Util.last(query.rels)));
        final CheetahQuery projectCheetahQuery = CheetahQuery.extendQuery(query, newProject);
        final RelNode newAggregate = aggregate.copy(aggregate.getTraitSet(),
                ImmutableList.of(Util.last(projectCheetahQuery.rels)));
        call.transformTo(CheetahQuery.extendQuery(projectCheetahQuery, newAggregate));
    }

    /* To be a valid Project, we allow it to contain references, and a single call
     * to an FLOOR function on the timestamp column. Returns the reference to
     * the timestamp, if any. */
    private static int validProject(Project project, CheetahQuery query) {
        List<RexNode> nodes = project.getProjects();
        int idxTimestamp = -1;
        for (int i = 0; i < nodes.size(); i++) {
            final RexNode e = nodes.get(i);
            if (e instanceof RexCall) {
                // It is a call, check that it is EXTRACT and follow-up conditions
                final RexCall call = (RexCall) e;
                if (CheetahDateTimeUtils.extractGranularity(call) == null) {
                    return -1;
                }
                if (idxTimestamp != -1) {
                    // Already one usage of timestamp column
                    return -1;
                }
                if (!(call.getOperands().get(0) instanceof RexInputRef)) {
                    return -1;
                }
                final RexInputRef ref = (RexInputRef) call.getOperands().get(0);
                if (!(CheetahRules.checkTimestampRefOnQuery(ImmutableBitSet.of(ref.getIndex()),
                        query.getTopNode(), query))) {
                    return -1;
                }
                idxTimestamp = i;
                continue;
            }
            if (!(e instanceof RexInputRef)) {
                // It needs to be a reference
                return -1;
            }
            final RexInputRef ref = (RexInputRef) e;
            if (CheetahRules.checkTimestampRefOnQuery(ImmutableBitSet.of(ref.getIndex()),
                    query.getTopNode(), query)) {
                if (idxTimestamp != -1) {
                    // Already one usage of timestamp column
                    return -1;
                }
                idxTimestamp = i;
            }
        }
        return idxTimestamp;
    }

    private static boolean validAggregate(Aggregate aggregate, int idx) {
        if (!aggregate.getGroupSet().get(idx)) {
            return false;
        }
        for (AggregateCall aggCall : aggregate.getAggCallList()) {
            if (aggCall.getArgList().contains(idx)) {
                return false;
            }
        }
        return true;
    }
}
