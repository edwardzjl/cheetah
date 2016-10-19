package cn.edu.zju.cheetah.jdbc.adapter.rules;

import cn.edu.zju.cheetah.jdbc.adapter.CheetahQuery;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.rules.ProjectSortTransposeRule;

/**
 * Rule to push back {@link org.apache.calcite.rel.core.Project} through a
 * {@link org.apache.calcite.rel.core.Sort}. Useful if after pushing Sort,
 * we could not push it inside CheetahQuery.
 */
public class CheetahSortProjectRule extends ProjectSortTransposeRule {
    CheetahSortProjectRule() {
        super(operand(Project.class, operand(Sort.class, operand(CheetahQuery.class, none()))));
    }
}
