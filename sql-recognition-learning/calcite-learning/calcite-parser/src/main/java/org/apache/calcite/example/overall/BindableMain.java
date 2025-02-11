package org.apache.calcite.example.overall;

import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.example.CalciteUtil;
import org.apache.calcite.interpreter.BindableConvention;
import org.apache.calcite.interpreter.BindableRel;
import org.apache.calcite.interpreter.Bindables;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;

/**
 * Arguments: <br>
 * args[0]: csv file path for user.csv <br>
 * args[1]: csv file path for order.csv <br>
 */
public class BindableMain {

    public static void main(String[] args) throws Exception {
        String userPath = "D:/workspace-alex/data_systems_learning_alex/sql-recognition-learning/calcite-learning/calcite-parser/src/main/resources/user.csv";
        String orderPath = "D:/workspace-alex/data_systems_learning_alex/sql-recognition-learning/calcite-learning/calcite-parser/src/main/resources/order.csv";
        SimpleTable userTable = SimpleTable.newBuilder("users")
                .addField("id", SqlTypeName.VARCHAR)
                .addField("name", SqlTypeName.VARCHAR)
                .addField("age", SqlTypeName.INTEGER)
                .withFilePath(userPath)
                .withRowCount(10)
                .build();
        SimpleTable orderTable = SimpleTable.newBuilder("orders")
                .addField("id", SqlTypeName.VARCHAR)
                .addField("user_id", SqlTypeName.VARCHAR)
                .addField("goods", SqlTypeName.VARCHAR)
                .addField("price", SqlTypeName.DECIMAL)
                .withFilePath(orderPath)
                .withRowCount(10)
                .build();
        SimpleSchema schema = SimpleSchema.newBuilder("s")
                .addTable(userTable)
                .addTable(orderTable)
                .build();
        CalciteSchema rootSchema = CalciteSchema.createRootSchema(false, false);
        rootSchema.add(schema.getSchemaName(), schema);

        String sql = "SELECT u.id, name, age, sum(price) " +
                "FROM users AS u join orders AS o ON u.id = o.user_id " +
                "WHERE age >= 20 AND age <= 30 " +
                "GROUP BY u.id, name, age " +
                "ORDER BY u.id";
        String sql1 = "SELECT id, name, age + 1 FROM users";
        String sql2 = "INSERT INTO users VALUES (1, 'Jark', 21)";
        String sql3 = "DELETE FROM users WHERE id > 1";

        Optimizer optimizer = Optimizer.create(rootSchema, schema.getSchemaName());
        // 1. SQL parse: SQL string --> SqlNode
        SqlNode sqlNode = optimizer.parse(sql);
        CalciteUtil.print("Parse result:", sqlNode.toString());
        // 2. SQL validate: SqlNode --> SqlNode
        SqlNode validateSqlNode = optimizer.validate(sqlNode);
        CalciteUtil.print("Validate result:", validateSqlNode.toString());
        // 3. SQL convert: SqlNode --> RelNode
        RelNode relNode = optimizer.convert(validateSqlNode);
        CalciteUtil.print("Convert result:", relNode.explain());
        // 4. SQL Optimize: RelNode --> RelNode
        RuleSet rules = RuleSets.ofList(
                CoreRules.FILTER_TO_CALC,
                CoreRules.PROJECT_TO_CALC,
                CoreRules.FILTER_CALC_MERGE,
                CoreRules.PROJECT_CALC_MERGE,
                CoreRules.FILTER_INTO_JOIN,
                Bindables.BindableTableScanRule.Config.DEFAULT.toRule(),
                Bindables.BindableProjectRule.DEFAULT_CONFIG.toRule(),

                EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE,
                EnumerableRules.ENUMERABLE_PROJECT_TO_CALC_RULE,
                EnumerableRules.ENUMERABLE_FILTER_TO_CALC_RULE,
                EnumerableRules.ENUMERABLE_JOIN_RULE,
                EnumerableRules.ENUMERABLE_SORT_RULE,
                EnumerableRules.ENUMERABLE_CALC_RULE,
                EnumerableRules.ENUMERABLE_AGGREGATE_RULE);
        RelNode optimizerRelTree = optimizer.optimize(
                relNode,
                relNode.getTraitSet().plus(BindableConvention.INSTANCE),
                rules);
        CalciteUtil.print("Optimize result:", optimizerRelTree.explain());
        // 5. SQL execute: RelNode --> execute code
        BindableRel bindableRel = (BindableRel) optimizerRelTree;

        Enumerable bind = bindableRel.bind(new SimpleDataContext(rootSchema.plus()));
        Enumerator enumerator = bind.enumerator();
        while (enumerator.moveNext()) {
            Object current = enumerator.current();
            Object[] values = (Object[]) current;
            StringBuilder sb = new StringBuilder();
            for (Object v : values) {
                sb.append(v).append(",");
            }
            sb.setLength(sb.length() - 1);
            System.out.println(sb);
        }
    }
}
