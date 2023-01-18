/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.sink.util;

import org.apache.kafka.connect.data.Struct;

/**
 * Description: ValueConverter interface
 *
 * @author douxin
 * @date 2023/01/14
 **/
interface ValueConverter {
    /**
     * Convert
     *
     * @param String the column name
     * @param Struct the struct value
     * @return
     */
    String convert(String columnName, Struct value);
}
