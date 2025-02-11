package org.apache.calcite.example.overall;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableInterpretable;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.example.CalciteUtil;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.runtime.Bindable;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;

import java.util.*;

/**
 * Arguments: <br>
 * args[0]: csv file path for user.csv <br>
 * args[1]: csv file path for order.csv <br>
 */
public class Main {

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
        rootSchema.plus().add("NOW", ScalarFunctionImpl.create(UDFNow.class, "now"));

        String sql = "SELECT u.id, name, age, sum(price) " +
                "FROM users AS u join orders AS o ON u.id = o.user_id " +
                "WHERE age >= ? AND age <= 30 " +
                "GROUP BY u.id, name, age " +
                "ORDER BY u.id";
        String sql1 = "SELECT id, name, age + 1 FROM users";
        String sql2 = "INSERT INTO users VALUES (1, 'Jark', 21)";
        String sql3 = "DELETE FROM users WHERE id > 1";
        String sql4 = "SELECT u.name, o.price FROM users AS u join orders AS o " +
                "on u.id = o.user_id WHERE o.price > 90";
        String sql5 = "select 1,2,5"; // 加入 ENUMERABLE_VALUES_RULE 规则后可以了
        String sql6 = "select NOW() as a, 1 from users"; //
        String sql7 = "select ABS(1)"; // 这样返回的不是一个数组，而是一个值
        String sql8 = "select ABS(age) from users"; // 这样返回的不是一个数组，而是一个值
        String sql9 = "select ABS(age),12 from users"; //
        String sql10 = "select NOW()"; // 无法优化

        Optimizer optimizer = Optimizer.create(rootSchema, schema.getSchemaName());
        // 1. SQL parse: SQL string --> SqlNode
        SqlNode sqlNode = optimizer.parse(sql5);
        CalciteUtil.print("Parse result:", sqlNode.toString());
        // 2. SQL validate: SqlNode --> SqlNode
        SqlNode validateSqlNode = optimizer.validate(sqlNode);
        CalciteUtil.print("Validate result:", validateSqlNode.toString());
        // 3. SQL convert: SqlNode --> RelNode
        RelNode relNode = optimizer.convert(validateSqlNode);
        CalciteUtil.print("Convert result:", relNode.explain());


        // 4. SQL Optimize: RelNode --> RelNode

        // 这里面基本都是 converter 规则
        List<RelOptRule> relOptRules = new ArrayList<>(EnumerableRules.ENUMERABLE_RULES);
        relOptRules.addAll(Arrays.asList(
                // 这两个都是 Transformation 规则
                EnumerableRules.ENUMERABLE_PROJECT_TO_CALC_RULE,
                EnumerableRules.ENUMERABLE_FILTER_TO_CALC_RULE,

                // 这些也是 Transformation 规则
                CoreRules.FILTER_TO_CALC,
                CoreRules.PROJECT_TO_CALC,
                CoreRules.FILTER_CALC_MERGE,
                CoreRules.PROJECT_CALC_MERGE,
                CoreRules.FILTER_INTO_JOIN
        ));

        // 要把 ENUMERABLE_VALUES_RULE 去掉，否则会报错
        // EnumerableProject 的 implement 方法中说，EnumerableCalcRel is always better
        relOptRules.remove(EnumerableRules.ENUMERABLE_PROJECT_RULE);

        // 这个是可以的
        List<RelOptRule> relOptRules2 = Arrays.asList(CoreRules.FILTER_TO_CALC,
                CoreRules.PROJECT_TO_CALC,
                CoreRules.FILTER_CALC_MERGE,
                CoreRules.PROJECT_CALC_MERGE,
                CoreRules.FILTER_INTO_JOIN,
                EnumerableRules.ENUMERABLE_VALUES_RULE,
                EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE,
                EnumerableRules.ENUMERABLE_PROJECT_TO_CALC_RULE,
                EnumerableRules.ENUMERABLE_FILTER_TO_CALC_RULE,
                EnumerableRules.ENUMERABLE_JOIN_RULE,
                EnumerableRules.ENUMERABLE_SORT_RULE,
                EnumerableRules.ENUMERABLE_CALC_RULE,
                EnumerableRules.ENUMERABLE_AGGREGATE_RULE);

        // 开始优化
        RuleSet rules = RuleSets.ofList(relOptRules);
        RelNode optimizerRelTree = optimizer.optimize(
                relNode,
                relNode.getTraitSet().plus(EnumerableConvention.INSTANCE),
                rules);
        CalciteUtil.print("Optimize result:", optimizerRelTree.explain());
        // 5. SQL execute: RelNode --> execute code
        EnumerableRel enumerable = (EnumerableRel) optimizerRelTree;


        Map<String, Object> internalParameters = new LinkedHashMap<>();

        EnumerableRel.Prefer prefer = EnumerableRel.Prefer.ARRAY;
        Bindable bindable = EnumerableInterpretable.toBindable(internalParameters,
                null, enumerable, prefer);
        Map<String, Object> dynamicParameters = new LinkedHashMap<>();
        dynamicParameters.put("?0", 24);
        Enumerable bind = bindable.bind(new SimpleDataContext(rootSchema.plus(), dynamicParameters));
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
