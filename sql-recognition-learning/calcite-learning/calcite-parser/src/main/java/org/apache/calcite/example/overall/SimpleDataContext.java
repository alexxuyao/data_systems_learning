package org.apache.calcite.example.overall;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.schema.SchemaPlus;
import scala.collection.parallel.immutable.ParRange;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class SimpleDataContext implements DataContext {

    private final SchemaPlus schema;

    private final Map<String, Object> dynamicParameters = new HashMap<>();

    public SimpleDataContext(SchemaPlus schema) {
        this.schema = schema;
    }

    public SimpleDataContext(SchemaPlus schema, Map<String, Object> dynamicParameters) {
        this.schema = schema;
        this.dynamicParameters.putAll(dynamicParameters);
    }

    @Override
    public SchemaPlus getRootSchema() {
        return schema;
    }

    @Override
    public JavaTypeFactory getTypeFactory() {
        return new JavaTypeFactoryImpl();
    }

    @Override
    public QueryProvider getQueryProvider() {
        throw new RuntimeException("un");
    }

    @Override
    public Object get(String name) {
        if (Variable.CANCEL_FLAG.camelName.equals(name)) {
            return new AtomicBoolean(false);
        }

        return dynamicParameters.get(name);
    }
}
