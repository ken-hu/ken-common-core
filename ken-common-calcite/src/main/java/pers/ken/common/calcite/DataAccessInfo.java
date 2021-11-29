package pers.ken.common.calcite;

/**
 * <code> DataAccessInfo </code>
 * <desc>  </desc>
 * <b>Creation Time:</b> 2021/11/29 21:35.
 *
 * @author _Ken.Hu
 */
public class DataAccessInfo {
    private String fieldName;
    private String tableName;
    private String condition;

    public DataAccessInfo(String fieldName, String tableName, String condition) {
        this.fieldName = fieldName;
        this.tableName = tableName;
        this.condition = condition;
    }

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getCondition() {
        return condition;
    }

    public void setCondition(String condition) {
        this.condition = condition;
    }
}
