package pers.ken.common.calcite;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelDataTypeSystemImpl;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.impl.SqlParserImpl;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.RelDecorrelator;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Properties;

/**
 * <code> CalciteTest </code>
 * <desc> CalciteTest </desc>
 * <b>Creation Time:</b> 11/23/2021 2:45 PM.
 *
 * @author _Ken.Hu
 */
public class CalciteTest {
    @Test
    public void customRuleTet() throws SqlParseException {
        String originalSql = "select u.id as user_id, u.name as user_name,  u.age as user_age \n" +
                "from users u\n" +
                "         join jobs j on u.id = j.id\n" +
                "where u.age > 30\n" +
                "  and u.age > 50\n" +
                "order by user_id\n" +
                "limit 10";

        SchemaPlus rootSchema = Frameworks.createRootSchema(true);

        rootSchema.add("JOBS", new AbstractTable() {
            @Override
            public RelDataType getRowType(final RelDataTypeFactory typeFactory) {
                RelDataTypeFactory.Builder builder = typeFactory.builder()
                        .add("ID", new BasicSqlType(new RelDataTypeSystemImpl() {
                        }, SqlTypeName.INTEGER))
                        .add("NAME", new BasicSqlType(new RelDataTypeSystemImpl() {
                        }, SqlTypeName.CHAR))
                        .add("COMPANY", new BasicSqlType(new RelDataTypeSystemImpl() {
                        }, SqlTypeName.CHAR));
                return builder.build();
            }
        });

        rootSchema.add("USERS", new AbstractTable() {
            @Override
            public RelDataType getRowType(final RelDataTypeFactory typeFactory) {
                RelDataTypeFactory.Builder builder = typeFactory.builder()
                        .add("ID", new BasicSqlType(new RelDataTypeSystemImpl() {
                        }, SqlTypeName.INTEGER))
                        .add("NAME", new BasicSqlType(new RelDataTypeSystemImpl() {
                        }, SqlTypeName.CHAR))
                        .add("AGE", new BasicSqlType(new RelDataTypeSystemImpl() {
                        }, SqlTypeName.INTEGER));
                return builder.build();
            }
        });


        SqlParser.Config sqlParserConfig = SqlParser.config()
                .withParserFactory(SqlParserImpl.FACTORY)
                .withCaseSensitive(false)
                .withQuoting(Quoting.BACK_TICK)
                .withQuotedCasing(Casing.TO_UPPER)
                .withUnquotedCasing(Casing.TO_UPPER)
                .withConformance(SqlConformanceEnum.ORACLE_12)
                .withLex(Lex.MYSQL);

        SqlParser sqlParser = SqlParser.create(originalSql, SqlParser.Config.DEFAULT);


        SqlNode sqlNode = sqlParser.parseQuery();

        SqlTypeFactoryImpl factory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);


        CalciteCatalogReader calciteCatalogReader = new CalciteCatalogReader(
                CalciteSchema.from(rootSchema),
                CalciteSchema.from(rootSchema).path(null),
                factory,
                new CalciteConnectionConfigImpl(new Properties()));

        final FrameworkConfig frameworkConfig = Frameworks.newConfigBuilder()
                .parserConfig(SqlParser.Config.DEFAULT)
                .traitDefs(ConventionTraitDef.INSTANCE, RelDistributionTraitDef.INSTANCE)
                .defaultSchema(rootSchema)
                .build();


        SqlValidator.Config sqlValidatorConfig = frameworkConfig.getSqlValidatorConfig();

        SqlValidator validator = SqlValidatorUtil.newValidator(SqlStdOperatorTable.instance(),
                calciteCatalogReader,
                factory,
                sqlValidatorConfig
//                SqlValidator.Config.DEFAULT
        );

        SqlNode validateSqlNode = validator.validate(sqlNode);
        final RexBuilder rexBuilder = new RexBuilder(factory);
        HepProgram hepProgram = HepProgram.builder()
//                .addRuleInstance(FilterJoinRule.FilterIntoJoinRule.FILTER_ON_JOIN)
//                .addRuleInstance(ReduceExpressionsRule.PROJECT_INSTANCE)
//                .addRuleInstance(CoreRules.AGGREGATE_REMOVE)
//                .addRuleInstance(CoreRules.PROJECT_FILTER_TRANSPOSE)
//                .addRuleInstance(CoreRules.MULTI_JOIN_BOTH_PROJECT)
//                .addRuleInstance(CoreRules.MULTI_JOIN_LEFT_PROJECT)
//                .addRuleInstance(CoreRules.MULTI_JOIN_OPTIMIZE)
//                .addRuleInstance(CoreRules.FILTER_INTO_JOIN)
//                .addMatchLimit(10)
//                .addRuleInstance(DataFilterRule.Config.DEFAULT.toRule())
                .build();
        HepPlanner hepPlanner = new HepPlanner(hepProgram);
        final RelOptCluster cluster = RelOptCluster.create(hepPlanner, rexBuilder);
        final SqlToRelConverter.Config config = SqlToRelConverter.config()
                .withTrimUnusedFields(false);
//        final SqlToRelConverter.Config config = frameworkConfig.getSqlToRelConverterConfig();

        final SqlToRelConverter sqlToRelConverter = new SqlToRelConverter(new DefaultViewExpander(),
                validator,
                calciteCatalogReader,
                cluster,
                StandardConvertletTable.INSTANCE,
                config);

        RelRoot root = sqlToRelConverter.convertQuery(validateSqlNode, false, true);


        root = root.withRel(sqlToRelConverter.flattenTypes(root.rel, true));
        final RelBuilder relBuilder = config.getRelBuilderFactory().create(cluster, null);
        root = root.withRel(RelDecorrelator.decorrelateQuery(root.rel, relBuilder));
        RelNode relNode = root.rel;

        hepPlanner.setRoot(relNode);
        hepPlanner.findBestExp();
        System.out.println(RelOptUtil.toString(relNode));
//        traverse(relNode);

    }

    private void traverse(RelNode relNode) {
        relNode.getInputs()
                .forEach(node -> {
                    if (node instanceof LogicalJoin) {
                        RelNode left = ((LogicalJoin) node).getLeft();
                        RelNode right = ((LogicalJoin) node).getRight();
                        traverse(left);
                        traverse(right);
                        String leftTable = left.getDigest();
                        String rightTable = right.getDigest();
                    }
                    if (node instanceof LogicalTableScan) {
                        RelOptTable table = node.getTable();
                    }
                    List<RelNode> inputs = node.getInputs();
                    inputs.forEach(this::traverse);
                });
    }

}
