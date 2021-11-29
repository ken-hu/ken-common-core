package pers.ken.common.calcite;

import com.alibaba.druid.sql.ast.SQLObject;
import com.alibaba.druid.sql.ast.expr.SQLInListExpr;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.dialect.postgresql.ast.stmt.PGSelectQueryBlock;
import com.alibaba.druid.sql.dialect.postgresql.ast.stmt.PGSelectStatement;
import com.alibaba.druid.sql.dialect.postgresql.visitor.PGASTVisitorAdapter;

import java.util.List;

/**
 * <code> DataFilterVisitor </code>
 * <desc> DataFilterVisitor </desc>
 * <b>Creation Time:</b> 11/29/2021 11:52 AM.
 *
 * @author _Ken.Hu
 */
public class DataFilterVisitor extends PGASTVisitorAdapter {

    private List<DataAccessInfo> dataAccessInfos;
    private DataAccessInfo dataAccessInfo;

    private String relTable;
    private String aliasTable;
    private String relField;
    private String aliasField;


    public DataFilterVisitor(List<DataAccessInfo> dataAccessInfos) {
        this.dataAccessInfos = dataAccessInfos;
    }


    /**
     * 获取表名信息
     *
     * @param x x
     * @return boolean
     */
    @Override
    public boolean visit(SQLExprTableSource x) {
        if (isTargetTableSource(x, dataAccessInfos)) {
            SQLObject parent = x.getParent();
            String alias = x.getAlias();
            System.out.println(alias);
            while (!(parent instanceof PGSelectQueryBlock) && parent != null) {
                parent = parent.getParent();
            }

            /* 插入行控制条件 */
            if (parent != null) {
                ((PGSelectQueryBlock) parent).addCondition(formCondition(alias));
            }
        }

        return super.visit(x);
    }


    @Override
    public boolean visit(PGSelectQueryBlock x) {
        return super.visit(x);
    }

    /**
     * 适合做列权限操作
     *
     * @param x x
     * @return boolean
     */
    @Override
    public boolean visit(PGSelectStatement x) {
        return super.visit(x);
    }

    @Override
    public void endVisit(SQLInListExpr x) {
        super.endVisit(x);
    }


    private boolean isTargetTableSource(SQLExprTableSource tableSource, List<DataAccessInfo> dataAccessInfos) {
        for (DataAccessInfo target : dataAccessInfos) {
            if (tableSource.getName().getSimpleName().equalsIgnoreCase(target.getTableName())) {
                dataAccessInfo = target;
                return true;
            }
        }
        return false;
    }

//    private String getAliasFieldName(String alias) {
//        return alias == null ? fieldName : alias + "." + fieldName;
//    }

    private String formCondition(String alias) {
        return alias == null ? dataAccessInfo.getFieldName() + " " + dataAccessInfo.getCondition() :
                alias + "." + dataAccessInfo.getFieldName() + " " + dataAccessInfo.getCondition();
    }

}
