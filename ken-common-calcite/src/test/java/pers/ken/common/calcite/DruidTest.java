package pers.ken.common.calcite;

import com.alibaba.druid.DbType;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * <code> DruidTest </code>
 * <desc>  </desc>
 * <b>Creation Time:</b> 11/21/2021 1:25 AM.
 *
 * @author _Ken.Hu
 */
public class DruidTest {

    private Map<String, String> privilegeMap = new HashMap<String, String>() {{
        put("users.city_id", "in('1','2')");
    }};

    @Test
    public void test() {
        String originalSql = "select u.id as user_id, u.name as user_name,  u.age as user_age \n" +
                "from users u\n" +
                "         join jobs j on u.id = j.id\n" +
                "where u.age > 30\n" +
                "  and u.age > 50\n" +
                "  and u.id in ('1','2','3')\n" +
                "order by user_id\n" +
                "limit 10";

        ArrayList<DataAccessInfo> dataAccessInfos = Lists.newArrayList(new DataAccessInfo("id", "users", " = 1"));
        DataFilterVisitor dataFilterVisitor = new DataFilterVisitor(dataAccessInfos);

        List<SQLStatement> statements = SQLUtils.parseStatements(originalSql, DbType.postgresql);
        statements.forEach(statement -> {
            statement.accept(dataFilterVisitor);
        });

//
//        SchemaStatVisitor schemaStatVisitor = SQLUtils.createSchemaStatVisitor(DbType.postgresql);
//        MySqlSchemaStatVisitor visitor = new MySqlSchemaStatVisitor();
//        List<SQLStatement> sqlStatements = SQLUtils.parseStatements(originalSql, DbType.postgresql);
//        sqlStatements.forEach(x -> {
//            x.accept(visitor);
//            Map<TableStat.Name, TableStat> tables = visitor.getTables();
//            System.out.println(tables);
//        });


        System.out.println(statements);

    }

}
