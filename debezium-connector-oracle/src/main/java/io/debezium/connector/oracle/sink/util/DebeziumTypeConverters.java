/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.sink.util;

import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

/**
 * Description: DebeziumValueConverters class
 *
 * @author gbase
 * @date 2023/08/07
 **/
public class DebeziumTypeConverters {
    private static final int BINARY_FLOAT = 100;
    private static final int BINARY_DOUBLE = 101;
    private static final int INTERVAL_DAY_TO_SECOND = -104;
    private static final int INTERVAL_YEAR_TO_MONTH = -103;
    private static final int TIMESTAMP_WITH_TIME_ZONE = -101;

    private static final Map<Integer, String> jdbcType2DataTypeMap = new HashMap<Integer, String>() {
        {
            // Character
            put(Types.CHAR, "CHAR");
            put(Types.NCHAR, "NCHAR");
            put(Types.NVARCHAR, "NVARCHAR2");
            put(Types.VARCHAR, "VARCHAR2");

            // Numeric
            put(BINARY_FLOAT, "FLOAT");
            put(BINARY_DOUBLE, "BINARY_DOUBLE");
            put(Types.NUMERIC, "NUMBER");
            put(Types.FLOAT, "FLOAT");

            // Temporal
            put(Types.DATE, "DATE");
            put(Types.TIMESTAMP, "TIMESTAMP");
            put(INTERVAL_DAY_TO_SECOND, "INTERVAL DAY TO SECOND");
            put(INTERVAL_YEAR_TO_MONTH, "INTERVAL YEAR TO MONTH");
            put(TIMESTAMP_WITH_TIME_ZONE, "TIMESTAMP WITH TIME ZONE");

            // Lob
            put(Types.BLOB, "BLOB");
            put(Types.CLOB, "CLOB");
        }
    };

    /**
     * To data type
     *
     * @param jdbcType the jdbc type
     * @return String
     */
    public static String toDataType(int jdbcType) {
        return jdbcType2DataTypeMap.getOrDefault(jdbcType, null);
    }
}
