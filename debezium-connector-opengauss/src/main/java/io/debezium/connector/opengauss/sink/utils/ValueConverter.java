package io.debezium.connector.opengauss.sink.utils;

import org.apache.kafka.connect.data.Struct;

/**
 * Description: ValueConverter interface
 *
 * @author wangzhengyuan
 * @date 2023/03/11
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
