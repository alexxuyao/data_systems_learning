package org.apache.calcite.example.overall;

import com.alibaba.fastjson.JSONObject;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class SimpleJsonEnumerator<E> implements Enumerator<E> {

    private final List<String> fieldNames;
    private final List<SqlTypeName> fieldTypes;

    private final List<JSONObject> data;

    private final AtomicBoolean cancelFlag;

    private E current;
    private int index = 0;

    public SimpleJsonEnumerator(List<String> fieldNames,
                                List<SqlTypeName> fieldTypes,
                                List<JSONObject> data,
                                AtomicBoolean cancelFlag) {
        this.fieldNames = fieldNames;
        this.fieldTypes = fieldTypes;
        this.data = data;
        this.cancelFlag = cancelFlag;
    }

    @Override
    public E current() {
        return current;
    }

    @Override
    public boolean moveNext() {

        if (cancelFlag.get()) {
            return false;
        }

        if (index < data.size()) {
            JSONObject jsonObject = data.get(index++);
            current = convertToRowData(jsonObject);
            return true;
        }

        current = null;
        return false;
    }

    private E convertToRowData(JSONObject jsonObject) {
        Object[] rowData = new Object[fieldNames.size()];

        for (int i = 0; i < fieldNames.size(); i++) {
            Object itemData = jsonObject.get(fieldNames.get(i));
            rowData[i] = convertByType(itemData, i);
        }

        return (E) rowData;
    }

    private Object convertByType(Object itemData, int index) {
        return itemData;
    }

    @Override
    public void reset() {
        index = 0;
    }

    @Override
    public void close() {
        // ignore
    }
}
