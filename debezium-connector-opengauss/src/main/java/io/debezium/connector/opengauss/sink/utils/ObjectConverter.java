/**
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.opengauss.sink.utils;

import org.apache.kafka.connect.data.Struct;

/**
 * Description: ObjectConverter interface
 *
 * @author czy
 * @date 2023/06/06
 **/
interface ObjectConverter {
    /**
     * Convert
     *
     * @param columnName the column name
     * @param value the column value
     * @param after struct value
     * @return new column value
     */
    String convert(String columnName, Object value, Struct after);
}
