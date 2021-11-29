package pers.ken.common.calcite;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

/**
 * <code> DefaultViewExpander </code>
 * <desc>  </desc>
 * <b>Creation Time:</b> 2021/11/29 21:42.
 *
 * @author _Ken.Hu
 */
public class DefaultViewExpander implements RelOptTable.ViewExpander {
    @Override
    public RelRoot expandView(RelDataType relDataType, String s, List<String> list, @Nullable List<String> list1) {
        return null;
    }
}
