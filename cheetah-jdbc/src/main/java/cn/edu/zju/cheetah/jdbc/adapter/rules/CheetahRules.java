package cn.edu.zju.cheetah.jdbc.adapter.rules;

import cn.edu.zju.cheetah.jdbc.adapter.CheetahDateTimeUtils;
import cn.edu.zju.cheetah.jdbc.adapter.CheetahQuery;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.trace.CalciteTrace;

import com.google.common.collect.ImmutableList;

import org.slf4j.Logger;

import java.util.List;
import java.util.function.Predicate;

/**
 * Rules and relational operators for {@link CheetahQuery}.
 */
public class CheetahRules {
  private CheetahRules() {}

  protected static final Logger LOGGER = CalciteTrace.getPlannerTracer();

  static final CheetahFilterRule FILTER = CheetahFilterRule.getInstance();
  static final CheetahProjectRule PROJECT = CheetahProjectRule.getInstance();
  static final CheetahAggregateRule AGGREGATE = CheetahAggregateRule.getInstance();
  static final CheetahProjectAggregateRule PROJECT_AGGREGATE = CheetahProjectAggregateRule.getInstance();
  static final CheetahSortRule SORT = CheetahSortRule.getInstance();
  static final CheetahProjectSortRule PROJECT_SORT = CheetahProjectSortRule.getInstance();
  static final CheetahSortProjectRule SORT_PROJECT = CheetahSortProjectRule.getInstance();

  public static final List<RelOptRule> RULES = ImmutableList.of(FILTER, PROJECT_AGGREGATE,
      PROJECT, AGGREGATE, PROJECT_SORT, SORT, SORT_PROJECT);

  /** Predicate that returns whether Cheetah can not handle an aggregate. */
  // edwardlol: change access modifiers from private to package-private
  // edwardlol: and some minor changes
  static final Predicate<AggregateCall> BAD_AGG = (aggregateCall -> {
    switch (aggregateCall.getAggregation().getKind())  {
      case COUNT:
      case SUM:
      case SUM0:
      case MIN:
      case MAX:
        return false;
      default:
        return true;
    }
  });

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
