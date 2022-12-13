/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.sink.util;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import io.debezium.data.Envelope;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.mysql.sink.object.ColumnMetaData;
import io.debezium.connector.mysql.sink.object.TableMetaData;

/**
 * Description: SqlTools class
 * @author douxin
 * @date 2022/10/31
 **/
public class SqlTools {
    private static final Logger LOGGER = LoggerFactory.getLogger(SqlTools.class);

    private Connection connection;

    public SqlTools(Connection connection) {
        this.connection = connection;
    }

    public TableMetaData getTableMetaData(String schemaName, String tableName) {
        List<ColumnMetaData> columnMetaDataList = new ArrayList<>();
        String sql = String.format(Locale.ENGLISH, "select column_name, data_type from " +
                "information_schema.columns where table_schema = '%s' and table_name = '%s'" +
                " order by ordinal_position;",
                schemaName, tableName);
        TableMetaData tableMetaData = null;
        try (Statement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery(sql)) {
            while (rs.next()) {
                columnMetaDataList.add(new ColumnMetaData(rs.getString("column_name"),
                        rs.getString("data_type")));
            }
            tableMetaData = new TableMetaData(schemaName, tableName, columnMetaDataList);
        }
        catch (SQLException exp) {
            LOGGER.error("SQL exception occurred, the sql statement is " + sql);
        }
        return tableMetaData;
    }

    public String getInsertSql(TableMetaData tableMetaData, Struct after) {
        StringBuilder sb = new StringBuilder();
        sb.append("insert into \"").append(tableMetaData.getSchemaName()).append("\".\"")
                .append(tableMetaData.getTableName()).append("\"").append(" values (");
        ArrayList<String> valueList = getValueList(tableMetaData, after, Envelope.Operation.CREATE);
        sb.append(String.join(", ", valueList));
        sb.append(");");
        return sb.toString();
    }

    public String getUpdateSql(TableMetaData tableMetaData, Struct before, Struct after) {
        List<ColumnMetaData> columnMetaDataList = tableMetaData.getColumnList();
        StringBuilder sb = new StringBuilder();
        sb.append("update \"").append(tableMetaData.getSchemaName()).append("\".\"")
                .append(tableMetaData.getTableName()).append("\"").append(" set ");
        ArrayList<String> updateSetValueList = getValueList(tableMetaData, after, Envelope.Operation.UPDATE);
        sb.append(String.join(", ", updateSetValueList));
        sb.append(" where ");
        ArrayList<String> whereConditionValueList = getValueList(tableMetaData, before, Envelope.Operation.DELETE);
        sb.append(String.join(" and ", whereConditionValueList));
        sb.append(";");
        return sb.toString();
    }

    public String getDeleteSql(TableMetaData tableMetaData, Struct before) {
        StringBuilder sb = new StringBuilder();
        sb.append("delete from \"").append(tableMetaData.getSchemaName()).append("\".\"")
                .append(tableMetaData.getTableName()).append("\"").append(" where ");
        ArrayList<String> whereConditionValueList = getValueList(tableMetaData, before, Envelope.Operation.DELETE);
        sb.append(String.join(" and ", whereConditionValueList));
        sb.append(";");
        return sb.toString();
    }

    private ArrayList<String> getValueList(TableMetaData tableMetaData, Struct after, Envelope.Operation operation) {
        ArrayList<String> valueList = new ArrayList<>();
        List<ColumnMetaData> columnMetaDataList = tableMetaData.getColumnList();
        String singleValue;
        String columnName;
        for (ColumnMetaData columnMetaData : columnMetaDataList) {
            singleValue = DebeziumValueConverters.getValue(columnMetaData, after);
            columnName = columnMetaData.getColumnName();
            switch (operation) {
                case CREATE:
                    valueList.add(singleValue);
                    break;
                case UPDATE:
                    valueList.add(columnName + " = " + singleValue);
                    break;
                case DELETE:
                    if (singleValue == null) {
                        valueList.add(columnName + " is null");
                    } else {
                        valueList.add(columnName + " = " + singleValue);
                    }
                    break;
            }
        }
        return valueList;
    }
}
