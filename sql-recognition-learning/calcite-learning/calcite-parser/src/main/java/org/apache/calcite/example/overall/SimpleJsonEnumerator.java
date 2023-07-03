package org.apache.calcite.example.overall;

import com.alibaba.fastjson.JSONObject;
import org.apache.calcite.linq4j.Enumerator;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class SimpleJsonEnumerator<E> implements Enumerator<E> {

    private final List<JSONObject> data;

    private final AtomicBoolean cancelFlag;

    private E current;
    private int index = 0;

    public SimpleJsonEnumerator(List<JSONObject> data,
                                AtomicBoolean cancelFlag) {
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
        return null;
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
