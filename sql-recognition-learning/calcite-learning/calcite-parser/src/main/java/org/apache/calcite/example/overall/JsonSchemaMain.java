package org.apache.calcite.example.overall;

import com.alibaba.fastjson.JSONObject;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.example.CalciteUtil;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.runtime.Bindable;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;

import java.util.*;

public class JsonSchemaMain {

    public static void main(String[] args) throws Exception {

        List<JSONObject> users = new ArrayList<>(Arrays.asList(
                new JSONObject() {{
                    put("id", "1");
                    put("name", "alex");
                    put("age", 20);
                }},
                new JSONObject() {{
                    put("id", "2");
                    put("name", "bob");
                    put("age", 30);
                }},
                new JSONObject() {{
                    put("id", "3");
                    put("name", "cindy");
                    put("age", 40);
                }}
        ));
        List<JSONObject> orders = new ArrayList<>(Arrays.asList(
                new JSONObject(){{
                    put("id", "1");
                    put("user_id", "1");
                    put("goods", "商品1");
                    put("price", "1.1");
                }},
                new JSONObject(){{
                    put("id", "2");
                    put("user_id", "1");
                    put("goods", "商品2");
                    put("price", "2.2");
                }},
                new JSONObject(){{
                    put("id", "3");
                    put("user_id", "2");
                    put("goods", "商品3");
                    put("price", "3.3");
                }},
                new JSONObject(){{
                    put("id", "4");
                    put("user_id", "2");
                    put("goods", "商品4");
                    put("price", "4.4");
                }},
                new JSONObject(){{
                    put("id", "5");
                    put("user_id", "3");
                    put("goods", "商品5");
                    put("price", "5.5");
                }},
                new JSONObject(){{
                    put("id", "6");
                    put("user_id", "3");
                    put("goods", "商品6");
                    put("price", "6.6");
                }}
        ));

        SimpleJsonTable userTable = new SimpleJsonTable("users",
                Arrays.asList("id", "name", "age"),
                Arrays.asList(SqlTypeName.VARCHAR, SqlTypeName.VARCHAR, SqlTypeName.INTEGER), users);

        SimpleJsonTable orderTable = new SimpleJsonTable("orders",
                Arrays.asList("id", "user_id", "goods", "price"),
                Arrays.asList(SqlTypeName.VARCHAR, SqlTypeName.VARCHAR, SqlTypeName.VARCHAR, SqlTypeName.BIGINT), orders);

        SimpleJsonSchema schema = SimpleJsonSchema.newBuilder("s")
                .addTable(userTable)
                .addTable(orderTable)
                .build();
        CalciteSchema rootSchema = CalciteSchema.createRootSchema(false, false);
        rootSchema.add(schema.getSchemaName(), schema);

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

        Optimizer optimizer = Optimizer.create(schema, schema.getSchemaName());
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
                EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE,
                EnumerableRules.ENUMERABLE_PROJECT_TO_CALC_RULE,
                EnumerableRules.ENUMERABLE_FILTER_TO_CALC_RULE,
                EnumerableRules.ENUMERABLE_JOIN_RULE,
                EnumerableRules.ENUMERABLE_SORT_RULE,
                EnumerableRules.ENUMERABLE_CALC_RULE,
                EnumerableRules.ENUMERABLE_AGGREGATE_RULE);
        RelNode optimizerRelTree = optimizer.optimize(
                relNode,
                relNode.getTraitSet().plus(EnumerableConvention.INSTANCE),
                rules);
        CalciteUtil.print("Optimize result:", optimizerRelTree.explain());
        // 5. SQL execute: RelNode --> execute code
        EnumerableRel enumerable = (EnumerableRel) optimizerRelTree;


        Map<String, Object> internalParameters = new LinkedHashMap<>();

        EnumerableRel.Prefer prefer = EnumerableRel.Prefer.ARRAY;

        BindableCodeGen.BindableCodeResult result = BindableCodeGen.toBindableCode(internalParameters, enumerable, prefer);

        System.out.println(result);

        Map<String, byte[]> classes = BindableCodeGen.compileBindable(result);
        Bindable bindable = BindableCodeGen.getBindable2(result, classes);

        Map<String, Object> dynamicParameters = new LinkedHashMap<>();
        dynamicParameters.put("?0", 19);
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
