package org.apache.calcite.example.overall;

import com.alibaba.fastjson.JSONObject;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.*;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class SimpleJsonTable extends AbstractTable implements ScannableTable {

    private final String tableName;
    private final List<String> fieldNames;
    private final List<SqlTypeName> fieldTypes;
    private final List<JSONObject> data;

    private RelDataType rowType;

    public SimpleJsonTable(String tableName,
                           List<String> fieldNames,
                           List<SqlTypeName> fieldTypes,
                           List<JSONObject> data) {
        this.tableName = tableName;
        this.fieldNames = fieldNames;
        this.fieldTypes = fieldTypes;
        this.data = data;
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root) {

        AtomicBoolean cancelFlag = DataContext.Variable.CANCEL_FLAG.get(root);

        return new AbstractEnumerable<Object[]>() {
            public Enumerator<Object[]> enumerator() {
                return new SimpleJsonEnumerator<>(fieldNames, fieldTypes, data, cancelFlag);
            }
        };
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        if (rowType == null) {
            List<RelDataTypeField> fields = new ArrayList<>(fieldNames.size());

            for (int i = 0; i < fieldNames.size(); i++) {
                RelDataType fieldType = typeFactory.createSqlType(fieldTypes.get(i));
                RelDataTypeField field = new RelDataTypeFieldImpl(fieldNames.get(i), i, fieldType);
                fields.add(field);
            }

            rowType = new RelRecordType(StructKind.PEEK_FIELDS, fields, false);
        }

        return rowType;
    }

    public String getTableName() {
        return tableName;
    }
}
