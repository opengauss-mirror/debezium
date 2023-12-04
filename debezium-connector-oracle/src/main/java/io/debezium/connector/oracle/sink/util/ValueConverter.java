/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.sink.util;

import org.apache.kafka.connect.data.Struct;

/**
 * Description: ValueConverter interface
 *
 * @author gbase
 * @date 2023/07/28
 **/
interface ValueConverter {
    /**
     * Convert
     *
     * @param columnName the column name
     * @param value the struct value
     * @return String
     */
    String convert(String columnName, Struct value);
}
