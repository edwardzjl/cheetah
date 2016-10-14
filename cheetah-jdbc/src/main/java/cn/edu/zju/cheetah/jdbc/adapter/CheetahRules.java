package cn.edu.zju.cheetah.jdbc.adapter;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.rules.ProjectSortTransposeRule;
import org.apache.calcite.rel.rules.SortProjectTransposeRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.trace.CalciteTrace;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import org.joda.time.Interval;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * Rules and relational operators for {@link CheetahQuery}.
 */
public class CheetahRules {
  private CheetahRules() {}

  protected static final Logger LOGGER = CalciteTrace.getPlannerTracer();

  public static final CheetahFilterRule FILTER = new CheetahFilterRule();
  public static final CheetahProjectRule PROJECT = new CheetahProjectRule();
  public static final CheetahAggregateRule AGGREGATE = new CheetahAggregateRule();
  public static final CheetahProjectAggregateRule
      PROJECT_AGGREGATE = new CheetahProjectAggregateRule();
  public static final CheetahSortRule SORT = new CheetahSortRule();
  public static final CheetahProjectSortRule PROJECT_SORT = new CheetahProjectSortRule();
  public static final CheetahSortProjectRule SORT_PROJECT = new CheetahSortProjectRule();

  public static final List<RelOptRule> RULES = ImmutableList.of(FILTER, PROJECT_AGGREGATE,
      PROJECT, AGGREGATE, PROJECT_SORT, SORT, SORT_PROJECT);

  /** Predicate that returns whether Cheetah can not handle an aggregate. */
  private static final Predicate<AggregateCall> BAD_AGG =
      new Predicate<AggregateCall>() {
        public boolean apply(AggregateCall aggregateCall) {
          switch (aggregateCall.getAggregation().getKind()) {
          case COUNT:
          case SUM:
          case SUM0:
          case MIN:
          case MAX:
            return false;
          default:
            return true;
          }
        }
      };

  /**
   * Rule to push a {@link org.apache.calcite.rel.core.Filter} into a {@link CheetahQuery}.
   */
  private static class CheetahFilterRule extends RelOptRule {
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

  /**
   * Rule to push a {@link org.apache.calcite.rel.core.Project} into a {@link CheetahQuery}.
   */
  private static class CheetahProjectRule extends RelOptRule {
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

  /**
   * Rule to push an {@link org.apache.calcite.rel.core.Aggregate} into a {@link CheetahQuery}.
   */
  private static class CheetahAggregateRule extends RelOptRule {
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
              || Iterables.any(aggregate.getAggCallList(), BAD_AGG)
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
      return !checkTimestampRefOnQuery(builder.build(), query.getTopNode(), query);
    }
  }

  /**
   * Rule to push an {@link org.apache.calcite.rel.core.Aggregate} and
   * {@link org.apache.calcite.rel.core.Project} into a {@link CheetahQuery}.
   */
  private static class CheetahProjectAggregateRule extends RelOptRule {
    private CheetahProjectAggregateRule() {
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
              || Iterables.any(aggregate.getAggCallList(), BAD_AGG)
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
          if (!(checkTimestampRefOnQuery(ImmutableBitSet.of(ref.getIndex()),
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
        if (checkTimestampRefOnQuery(ImmutableBitSet.of(ref.getIndex()),
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

  /**
   * Rule to push an {@link org.apache.calcite.rel.core.Sort} through a
   * {@link org.apache.calcite.rel.core.Project}. Useful to transform
   * to complex Cheetah queries.
   */
  private static class CheetahProjectSortRule extends SortProjectTransposeRule {
    private CheetahProjectSortRule() {
      super(operand(Sort.class, operand(Project.class, operand(CheetahQuery.class, none()))));
    }
  }

  /**
   * Rule to push back {@link org.apache.calcite.rel.core.Project} through a
   * {@link org.apache.calcite.rel.core.Sort}. Useful if after pushing Sort,
   * we could not push it inside CheetahQuery.
   */
  private static class CheetahSortProjectRule extends ProjectSortTransposeRule {
    private CheetahSortProjectRule() {
      super(operand(Project.class, operand(Sort.class, operand(CheetahQuery.class, none()))));
    }
  }

  /**
   * Rule to push an {@link org.apache.calcite.rel.core.Aggregate} into a {@link CheetahQuery}.
   */
  private static class CheetahSortRule extends RelOptRule {
    private CheetahSortRule() {
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
                checkTimestampRefOnQuery(positionsReferenced.build(), topAgg.getInput(), query);
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

  /* Check if any of the references leads to the timestamp column */
  private static boolean checkTimestampRefOnQuery(ImmutableBitSet set, RelNode top,
      CheetahQuery query) {
    if (top instanceof Project) {
      ImmutableBitSet.Builder newSet = ImmutableBitSet.builder();
      final Project project = (Project) top;
      for (int index : set) {
        RexNode node = project.getProjects().get(index);
        if (node instanceof RexInputRef) {
          newSet.set(((RexInputRef) node).getIndex());
        } else if (node instanceof RexCall) {
          RexCall call = (RexCall) node;
          assert CheetahDateTimeUtils.extractGranularity(call) != null;
          newSet.set(((RexInputRef) call.getOperands().get(0)).getIndex());
        }
      }
      top = project.getInput();
      set = newSet.build();
    }

    // Check if any references the timestamp column
    for (int index : set) {
      if (query.cheetahTable.timestampFieldName.equals(
              top.getRowType().getFieldNames().get(index))) {
        return true;
      }
    }

    return false;
  }
}

// End CheetahRules.java
