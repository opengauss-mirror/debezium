/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.sink.util;

import java.util.Collection;
import java.util.function.Function;

/**
 * Description: SqlStatementBuilder class
 *
 * @author gbase
 * @date 2023/08/07
 **/
public class SqlStatementBuilder {
    private final StringBuilder builder;
    public SqlStatementBuilder() {
        this.builder = new StringBuilder();
    }

    /**
     * Append value
     *
     * @param value the value
     * @return SqlStatementBuilder
     */
    public SqlStatementBuilder append(Object value) {
        builder.append(value);
        return this;
    }

    /**
     * Append list
     *
     * @param columnNames the column name
     * @param transform  the transform
     * @return SqlStatementBuilder
     */
    public SqlStatementBuilder appendList(Collection<String> columnNames, Function<String, String> transform) {
        appendList(",", columnNames, transform);
        return this;
    }

    /**
     * Append list
     *
     * @param delimiter the delimiter
     * @param columnNames the column name
     * @param transform the transform
     * @return SqlStatementBuilder
     */
    public SqlStatementBuilder appendList(String delimiter, Collection<String> columnNames, Function<String, String> transform) {
        boolean appended = false;
        for (String columnName : columnNames) {
            String apply = transform.apply(columnName);
            if (!apply.trim().isEmpty()) {
                builder.append(apply);
                builder.append(delimiter);
                appended = true;
            }
        }
        if (appended) {
            builder.deleteCharAt(builder.length() - 1);
        }
        return this;
    }

    /**
     * Build result
     *
     * @return String
     */
    public String build() {
        return builder.toString();
    }
}
