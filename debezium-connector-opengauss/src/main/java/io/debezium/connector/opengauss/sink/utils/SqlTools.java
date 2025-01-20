/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.opengauss.sink.utils;

import io.debezium.connector.opengauss.sink.object.ColumnMetaData;
import io.debezium.connector.opengauss.sink.object.TableMetaData;
import io.debezium.data.Envelope;
import org.apache.kafka.connect.data.Struct;

import java.util.List;

public abstract class SqlTools {
    /**
     * Get wrapped name
     *
     * @paran name the name
     * @return String the wrapped name
     */
    public String getWrappedName(String name) {
        return "`" + name + "`";
    }

    /**
     * Get table full name
     *
     * @param TableMetaData the table metadata
     * @return String the table Full name
     */
    public String getTableFullName(TableMetaData tableMetaData) {
        return getWrappedName(tableMetaData.getSchemaName()) + "." + getWrappedName(tableMetaData.getTableName());
    }

    public abstract TableMetaData getTableMetaData(String schemaName, String table);

    public abstract Boolean getIsConnection();

    public abstract String getInsertSql(TableMetaData tableMetaData, Struct after);

    public abstract String getUpdateSql(TableMetaData tableMetaData, Struct before, Struct after);

    public abstract String getDeleteSql(TableMetaData tableMetaData, Struct before);

    public abstract String sqlAddBitCast(TableMetaData tableMetaData, String columnString, String loadSql);

    public abstract List<String> conversionFullData(List<ColumnMetaData> columnList, List<String> lineList, Struct after);

    public abstract String getReadSql(TableMetaData tableMetaData, Struct after, Envelope.Operation create);

    public abstract boolean isExistSql(String sql);

    public abstract List<String> getReadSqlForUpdate(TableMetaData tableMetaData, Struct before, Struct after);

    public abstract List<String> getForeignTableList(String tableFullName);

    /**
     * Get full migrate sql
     *
     * @param tableFullName tableFullName
     * @return Executable SQL statements
     */
    public String loadFullSql(String tableFullName) {
        return null;
    }
}
