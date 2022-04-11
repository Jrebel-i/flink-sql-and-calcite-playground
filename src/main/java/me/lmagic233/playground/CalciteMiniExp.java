package me.lmagic233.playground;

import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFactory.Builder;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorCatalogReader;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.table.planner.parse.CalciteParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.Set;

/**
 * @author kangqi
 * @date 2021/8/25
 */
@SuppressWarnings("deprecation")
public class CalciteMiniExp {
  private static final Logger LOGGER = LoggerFactory.getLogger(CalciteMiniExp.class);
  private static final RelDataTypeFactory DEFAULT_TYPE_FACTORY = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
  private static final RelDataTypeSystem DEFAULT_TYPE_SYSTEM = RelDataTypeSystem.DEFAULT;

  public static void main(String[] args) throws Exception {
    // ------------ Schema ------------
    SchemaPlus rootSchema = Frameworks.createRootSchema(true);

    rootSchema.add("student", new AbstractTable() {
      @Override
      public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        RelDataTypeFactory.Builder builder = new Builder(DEFAULT_TYPE_FACTORY);

        builder.add("id", new BasicSqlType(DEFAULT_TYPE_SYSTEM, SqlTypeName.BIGINT));
        builder.add("name", new BasicSqlType(DEFAULT_TYPE_SYSTEM, SqlTypeName.VARCHAR));
        builder.add("class", new BasicSqlType(DEFAULT_TYPE_SYSTEM, SqlTypeName.VARCHAR));
        builder.add("age", new BasicSqlType(DEFAULT_TYPE_SYSTEM, SqlTypeName.INTEGER));

        return builder.build();
      }
    });

    rootSchema.add("exam_result", new AbstractTable() {
      @Override
      public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        RelDataTypeFactory.Builder builder = new Builder(DEFAULT_TYPE_FACTORY);

        builder.add("student_id", new BasicSqlType(DEFAULT_TYPE_SYSTEM, SqlTypeName.BIGINT));
        builder.add("score1", new BasicSqlType(DEFAULT_TYPE_SYSTEM, SqlTypeName.FLOAT));
        builder.add("score2", new BasicSqlType(DEFAULT_TYPE_SYSTEM, SqlTypeName.FLOAT));

        return builder.build();
      }
    });

    String sql = /* language=SQL */
      "SELECT a.id, a.name, SUM(b.score1 * 0.7 + b.score2 * 0.3) AS total_score " +
        "FROM student a " +
        "INNER JOIN exam_result b ON a.id = b.student_id " +
        "WHERE a.age < 20 AND b.score1 > 60.0 " +
        "GROUP BY a.id, a.name";

    // ------------ Parsing ------------
    FrameworkConfig frameworkConfig = Frameworks.newConfigBuilder()
      .parserConfig(SqlParser.config().withCaseSensitive(false).withLex(Lex.MYSQL_ANSI))
      .defaultSchema(rootSchema)
      .build();

    SqlParser parser = SqlParser.create(sql);
    SqlNode originalSqlNode = parser.parseStmt();

    LOGGER.warn("--- Original SqlNode ---");
    LOGGER.error(originalSqlNode.toString());

    // ------------ Validation ------------
    Properties cxnConfig = new Properties();
    cxnConfig.setProperty(
      CalciteConnectionProperty.CASE_SENSITIVE.camelName(),
      String.valueOf(frameworkConfig.getParserConfig().caseSensitive()));

    CalciteCatalogReader catalogReader = new CalciteCatalogReader(
      CalciteSchema.from(rootSchema),
      CalciteSchema.from(frameworkConfig.getDefaultSchema()).path(null),
      DEFAULT_TYPE_FACTORY,
      new CalciteConnectionConfigImpl(cxnConfig)
    );

    SqlValidator validator = new SqlValidatorImpl1(
      frameworkConfig.getOperatorTable(),
      catalogReader,
      DEFAULT_TYPE_FACTORY
    );

    SqlNode validatedSqlNode = validator.validate(originalSqlNode);

    LOGGER.warn("--- Validated SqlNode ---");
    LOGGER.error(validatedSqlNode.toString());

    // ------------ Planning ------------
    // Do not care about the `VolcanoPlanner` here for now
    RelOptCluster relOptCluster = RelOptCluster.create(new VolcanoPlanner(), new RexBuilder(DEFAULT_TYPE_FACTORY));

    SqlToRelConverter relConverter = new SqlToRelConverter(
      null,
      validator,
      catalogReader,
      relOptCluster,
      frameworkConfig.getConvertletTable()
    );

    RelRoot relRoot = relConverter.convertQuery(validatedSqlNode, false, true);
    RelNode originalRelNode = relRoot.rel;

    LOGGER.warn("--- Original RelNode ---");
    LOGGER.error(RelOptUtil.toString(originalRelNode));

    // ------------ Optimization ------------
    HepProgram hepProgram = new HepProgramBuilder()
      .addRuleInstance(CoreRules.FILTER_INTO_JOIN)
      // .addRuleInstance(...)
      .addMatchOrder(HepMatchOrder.BOTTOM_UP)
      .build();

    HepPlanner hepPlanner = new HepPlanner(hepProgram);
    hepPlanner.setRoot(originalRelNode);
    RelNode optimizedRelNode = hepPlanner.findBestExp();

    LOGGER.warn("--- Optimized RelNode ---");
    LOGGER.error(RelOptUtil.toString(optimizedRelNode));
  }

  // Leverage protected constructor
  static class SqlValidatorImpl1 extends SqlValidatorImpl {
    SqlValidatorImpl1(
      SqlOperatorTable opTab,
      SqlValidatorCatalogReader catalogReader,
      RelDataTypeFactory typeFactory) {
      super(opTab, catalogReader, typeFactory, Config.DEFAULT);
    }
  }
}
