package org.apache.calcite.example.overall;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.*;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.util.ListSqlOperatorTable;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class Optimizer {

  private final CalciteConnectionConfig config;
  private final SqlValidator validator;
  private final SqlToRelConverter converter;
  private final VolcanoPlanner planner;

  public Optimizer(
          CalciteConnectionConfig config,
          SqlValidator validator,
          SqlToRelConverter converter,
          VolcanoPlanner planner) {
    this.config = config;
    this.validator = validator;
    this.converter = converter;
    this.planner = planner;
  }

  public static Optimizer create(CalciteSchema rootSchema, String schemaName) {
    Properties configProperties = new Properties();
    configProperties.put(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), Boolean.TRUE.toString());
    configProperties.put(CalciteConnectionProperty.UNQUOTED_CASING.camelName(), Casing.UNCHANGED.toString());
    configProperties.put(CalciteConnectionProperty.QUOTED_CASING.camelName(), Casing.UNCHANGED.toString());
    // configProperties.put(CalciteConnectionProperty.FUN.camelName(), SqlLibrary.MYSQL.fun); 加了也没用？
    CalciteConnectionConfig config = new CalciteConnectionConfigImpl(configProperties);

    // create type factory, needed by SqlValidator
    RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();

    // create catalog reader, needed by SqlValidator
    Prepare.CatalogReader catalogReader = new CalciteCatalogReader(
            rootSchema,
            Collections.singletonList(schemaName),
            typeFactory,
            config);

    // create SqlValidator
    SqlValidator.Config validatorConfig = SqlValidator.Config.DEFAULT
            .withLenientOperatorLookup(config.lenientOperatorLookup())
            .withSqlConformance(config.conformance())
            .withDefaultNullCollation(config.defaultNullCollation())
            .withIdentifierExpansion(true);

    // 组合自定义的operator和标准的operator
    List<SqlOperator> operatorList = new ArrayList<>(catalogReader.getOperatorList());
    operatorList.addAll(SqlStdOperatorTable.instance().getOperatorList());
    ListSqlOperatorTable operatorTable = new ListSqlOperatorTable(operatorList);

    // create SqlValidator
    SqlValidator validator = SqlValidatorUtil.newValidator(
            operatorTable, catalogReader, typeFactory, validatorConfig);

    // create VolcanoPlanner, needed by SqlToRelConverter and optimizer
    VolcanoPlanner planner = new VolcanoPlanner(RelOptCostImpl.FACTORY, Contexts.of(config));
    planner.addRelTraitDef(ConventionTraitDef.INSTANCE);

    // create SqlToRelConverter
    RelOptCluster cluster = RelOptCluster.create(planner, new RexBuilder(typeFactory));
    SqlToRelConverter.Config converterConfig = SqlToRelConverter.config()
            .withTrimUnusedFields(true)
            .withExpand(false);
    SqlToRelConverter converter = new SqlToRelConverter(
            null,
            validator,
            catalogReader,
            cluster,
            StandardConvertletTable.INSTANCE,
            converterConfig);

    return new Optimizer(config, validator, converter, planner);
  }

  public SqlNode parse(String sql) throws Exception {
    SqlParser.Config parserConfig = SqlParser.config()
            .withQuotedCasing(config.quotedCasing())
            .withUnquotedCasing(config.unquotedCasing())
            .withQuoting(config.quoting())
            .withConformance(config.conformance())
            .withCaseSensitive(config.caseSensitive());
    SqlParser parser = SqlParser.create(sql, parserConfig);

    return parser.parseStmt();
  }

  public SqlNode validate(SqlNode node) {
    return validator.validate(node);
  }

  /**
   * 将 语法树 转为 关系代数 树
   * @param node 语法树
   * @return 关系代数树
   */
  public RelNode convert(SqlNode node) {
    RelRoot root = converter.convertQuery(node, false, true);
    return root.rel;
  }

  /**
   * 优化关系代数树
   * 是 逻辑计划优化 还是 物理计划优化 ？
   *
   * @param node 关系代数树
   * @param requiredTraitSet
   * @param rules 优化规则
   * @return 优化后的关系代数树
   */
  public RelNode optimize(RelNode node, RelTraitSet requiredTraitSet, RuleSet rules) {
    Program program = Programs.of(RuleSets.ofList(rules));

    return program.run(
            planner,
            node,
            requiredTraitSet,
            Collections.emptyList(),
            Collections.emptyList());
  }
}
