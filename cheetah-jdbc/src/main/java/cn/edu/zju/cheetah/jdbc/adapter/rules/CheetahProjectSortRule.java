package cn.edu.zju.cheetah.jdbc.adapter.rules;

import cn.edu.zju.cheetah.jdbc.adapter.CheetahQuery;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.rules.SortProjectTransposeRule;

/**
 * Rule to push an {@link org.apache.calcite.rel.core.Sort} through a
 * {@link org.apache.calcite.rel.core.Project}. Useful to transform
 * to complex Cheetah queries.
 */
class CheetahProjectSortRule extends SortProjectTransposeRule {

    private static final CheetahProjectSortRule instance;
    static {
        instance = new CheetahProjectSortRule();
    }

    static CheetahProjectSortRule getInstance() {
        return instance;
    }

    private CheetahProjectSortRule() {
        super(operand(Sort.class, operand(Project.class, operand(CheetahQuery.class, none()))));
    }
}
