package cn.edu.zju.cheetah.jdbc.adapter.rules;

import cn.edu.zju.cheetah.jdbc.adapter.CheetahDateTimeUtils;
import cn.edu.zju.cheetah.jdbc.adapter.CheetahQuery;
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
  // edwardlol: change access modifiers from private to package-private
  static final Predicate<AggregateCall> BAD_AGG =
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



  /* Check if any of the references leads to the timestamp column */
  // edwardlol: change access modifiers from private to package-private
  static boolean checkTimestampRefOnQuery(ImmutableBitSet set, RelNode top,
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
