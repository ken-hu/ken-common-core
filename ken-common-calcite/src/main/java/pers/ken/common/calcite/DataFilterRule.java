package pers.ken.common.calcite;

import com.alibaba.druid.DbType;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLObject;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLSelect;
import com.alibaba.druid.sql.dialect.oracle.ast.stmt.OracleSelectQueryBlock;
import com.alibaba.druid.sql.dialect.oracle.ast.stmt.OracleSelectTableReference;
import com.alibaba.druid.sql.dialect.postgresql.ast.stmt.PGSelectStatement;
import com.alibaba.druid.sql.dialect.postgresql.visitor.PGASTVisitorAdapter;
import com.alibaba.druid.util.JdbcConstants;

import java.util.List;

/**
 * <code> DataFilterRule </code>
 * <desc>  </desc>
 * <b>Creation Time:</b> 2021/11/28 21:04.
 *
 * @author _Ken.Hu
 */
public class DataFilterRule extends PGASTVisitorAdapter {
    private String fieldName;
    private String condition;
    private String[] tables;
    private final DbType dbType = JdbcConstants.POSTGRESQL;


    @Override
    public boolean visit(PGSelectStatement x) {
        SQLSelect select = x.getSelect();
        List<String> alias = select.computeSelecteListAlias();
        System.out.println(alias);
        select.addWhere(SQLUtils.toSQLExpr("a.name = '2'"));
        return super.visit(x);
    }

    @Override
    public boolean visit(SQLExprTableSource x) {
        String alias = x.getAlias();
        String aliasFieldName = getAliasFieldName(alias);
        System.out.println(aliasFieldName);
        return super.visit(x);
    }



    // 出口状态
    public boolean visit(OracleSelectTableReference selectTableReferenece) {
        /* 符合目标表源名 */
        if (isTargetTableSource(selectTableReferenece, tables)) {
            SQLObject parent = selectTableReferenece.getParent();
            String alias = selectTableReferenece.getAlias();

            /* 回溯到选择语句 */
            while (!(parent instanceof OracleSelectQueryBlock) && parent != null) {
                parent = parent.getParent();
            }

            /* 插入行控制条件 */
            if (parent != null) {
                ((OracleSelectQueryBlock) parent).addCondition(formCondition(alias));
            }
        }

        return false;
    }

    /* 判断是否与目标表名一致 */
    private boolean isTargetTableSource(SQLExprTableSource tableSource, String[] targetTables) {
        for (String target : targetTables) {
            if (tableSource.getName().getSimpleName().toString().equalsIgnoreCase(target)) {
                return true;
            }
        }
        return false;
    }

    /* 别名条件 */
    private String getAliasFieldName(String alias) {
        return alias == null ? fieldName : alias + "." + fieldName;
    }

    /* 组成条件 */
    private String formCondition(String alias) {
        return alias == null ? fieldName + " " + condition :
                alias + "." + fieldName + " " + condition;
    }

}
