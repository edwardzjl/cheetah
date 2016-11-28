package cn.edu.zju.cheetah.jdbc.adapter.rules;

import cn.edu.zju.cheetah.jdbc.adapter.CheetahQuery;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import java.util.ArrayList;
import java.util.List;

/**
 * Rule to push a {@link org.apache.calcite.rel.core.Project} into a {@link CheetahQuery}.
 */
class CheetahProjectRule extends RelOptRule {

    private static final CheetahProjectRule instance;
    static {
        instance = new CheetahProjectRule();
    }

    static CheetahProjectRule getInstance() {
        return instance;
    }

    private CheetahProjectRule() {
        super(operand(Project.class, operand(CheetahQuery.class, none())));
    }

    public void onMatch(RelOptRuleCall call) {
        final Project project = call.rel(0);
        final CheetahQuery query = call.rel(1);
        if (!CheetahQuery.isValidSignature(query.signature() + 'p')) {
            return;
        }

        if (canProjectAll(project.getProjects())) {
            // All expressions can be pushed to Cheetah in their entirety.
            final RelNode newProject = project.copy(project.getTraitSet(),
                    ImmutableList.of(Util.last(query.rels)));
            RelNode newNode = CheetahQuery.extendQuery(query, newProject);
            call.transformTo(newNode);
            return;
        }
        final Pair<List<RexNode>, List<RexNode>> pair = splitProjects(
                project.getCluster().getRexBuilder(), query, project.getProjects());
        if (pair == null) {
            // We can't push anything useful to Cheetah.
            return;
        }
        final List<RexNode> above = pair.left;
        final List<RexNode> below = pair.right;
        final RelDataTypeFactory.FieldInfoBuilder builder = project.getCluster().getTypeFactory()
                .builder();
        final RelNode input = Util.last(query.rels);
        for (RexNode e : below) {
            final String name;
            if (e instanceof RexInputRef) {
                name = input.getRowType().getFieldNames().get(((RexInputRef) e).getIndex());
            } else {
                name = null;
            }
            builder.add(name, e.getType());
        }
        final RelNode newProject = project.copy(project.getTraitSet(), input, below, builder.build());
        final CheetahQuery newQuery = CheetahQuery.extendQuery(query, newProject);
        final RelNode newProject2 = project.copy(project.getTraitSet(), newQuery, above,
                project.getRowType());
        call.transformTo(newProject2);
    }

    private static boolean canProjectAll(List<RexNode> nodes) {
        for (RexNode e : nodes) {
            if (!(e instanceof RexInputRef)) {
                return false;
            }
        }
        return true;
    }

    private static Pair<List<RexNode>, List<RexNode>> splitProjects(final RexBuilder rexBuilder,
                                                                    final RelNode input, List<RexNode> nodes) {
        final RelOptUtil.InputReferencedVisitor visitor = new RelOptUtil.InputReferencedVisitor();
        for (RexNode node : nodes) {
            node.accept(visitor);
        }
        if (visitor.inputPosReferenced.size() == input.getRowType().getFieldCount()) {
            // All inputs are referenced
            return null;
        }
        final List<RexNode> belowNodes = new ArrayList<>();
        final List<RelDataType> belowTypes = new ArrayList<>();
        final List<Integer> positions = Lists.newArrayList(visitor.inputPosReferenced);
        for (int i : positions) {
            final RexNode node = rexBuilder.makeInputRef(input, i);
            belowNodes.add(node);
            belowTypes.add(node.getType());
        }
        final List<RexNode> aboveNodes = new ArrayList<>();
        for (RexNode node : nodes) {
            aboveNodes.add(
                    node.accept(
                            new RexShuttle() {
                                @Override public RexNode visitInputRef(RexInputRef ref) {
                                    final int index = positions.indexOf(ref.getIndex());
                                    return rexBuilder.makeInputRef(belowTypes.get(index), index);
                                }
                            }));
        }
        return Pair.of(aboveNodes, belowNodes);
    }
}
