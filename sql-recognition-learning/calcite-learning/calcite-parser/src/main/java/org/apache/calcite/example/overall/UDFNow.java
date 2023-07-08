package org.apache.calcite.example.overall;

import java.sql.Timestamp;

public class UDFNow {

    public Timestamp now(){
        return new Timestamp(System.currentTimeMillis());
    }
}
