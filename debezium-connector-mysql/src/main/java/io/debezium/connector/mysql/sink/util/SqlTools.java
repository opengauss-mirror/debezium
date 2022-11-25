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
        List<ColumnMetaData> columnMetaDataList = tableMetaData.getColumnList();
        StringBuilder sb = new StringBuilder();
        sb.append("insert into \"").append(tableMetaData.getSchemaName()).append("\".\"")
                .append(tableMetaData.getTableName()).append("\"").append(" values (");
        for (ColumnMetaData columnMetaData : columnMetaDataList) {
            String columnName = columnMetaData.getColumnName();
            String columnType = columnMetaData.getColumnType();
            Object originValue = after.get(columnName);
            if (originValue == null) {
                sb.append(originValue).append(", ");
            }
            else {
                String value = getSingleValue(columnType, originValue);
                sb.append(value).append(", ");
            }
        }
        sb.deleteCharAt(sb.length() - 1);
        sb.deleteCharAt(sb.length() - 1);
        sb.append(");");
        return sb.toString();
    }

    public String getDeleteSql(TableMetaData tableMetaData, Struct before) {
        List<ColumnMetaData> columnMetaDataList = tableMetaData.getColumnList();
        StringBuilder sb = new StringBuilder();
        sb.append("delete from \"").append(tableMetaData.getSchemaName()).append("\".\"")
                .append(tableMetaData.getTableName()).append("\"").append(" where ");
        for (ColumnMetaData columnMetaData : columnMetaDataList) {
            String columnName = columnMetaData.getColumnName();
            String columnType = columnMetaData.getColumnType();
            Object originValue = before.get(columnName);
            if (originValue == null) {
                sb.append(columnName).append(" is null ").append(" and ");
            }
            else {
                String value = getSingleValue(columnType, originValue);
                sb.append(columnName).append(" = ").append(value).append(" and ");
            }
        }
        sb.delete(sb.length() - 5, sb.length());
        sb.append(";");
        return sb.toString();
    }

    public String getUpdateSql(TableMetaData tableMetaData, Struct before, Struct after) {
        List<ColumnMetaData> columnMetaDataList = tableMetaData.getColumnList();
        StringBuilder sb = new StringBuilder();
        sb.append("update \"").append(tableMetaData.getSchemaName()).append("\".\"")
                .append(tableMetaData.getTableName()).append("\"").append(" set ");
        for (ColumnMetaData columnMetaData : columnMetaDataList) {
            String columnName = columnMetaData.getColumnName();
            String columnType = columnMetaData.getColumnType();
            Object originValue = after.get(columnName);
            if (originValue == null) {
                sb.append(columnName).append(" = ").append(originValue).append(", ");
            }
            else {
                String value = getSingleValue(columnType, originValue);
                sb.append(columnName).append(" = ").append(value).append(", ");
            }
        }
        sb.delete(sb.length() - 2, sb.length());
        sb.append(" where ");
        for (ColumnMetaData columnMetaData : columnMetaDataList) {
            String columnName = columnMetaData.getColumnName();
            String columnType = columnMetaData.getColumnType();
            Object originValue = before.get(columnName);
            if (originValue == null) {
                sb.append(columnName).append(" is null ").append(" and ");
            }
            else {
                String value = getSingleValue(columnType, originValue);
                sb.append(columnName).append(" = ").append(value).append(" and ");
            }
        }
        sb.delete(sb.length() - 5, sb.length());
        sb.append(";");
        return sb.toString();
    }

    public String getSingleValue(String columnType, Object originValue) {
        switch (columnType) {
            case "int":
                return originValue.toString();
            case "char":
                return addingSingleQuotation(originValue);
            default:
                return addingSingleQuotation(originValue);
        }
    }

    public Object getSingleValue_new(String columnType, Object originValue) {
        switch (columnType) {
            case "int":
                return originValue.toString();
            case "char":
                return addingSingleQuotation(originValue);
            default:
                return addingSingleQuotation(originValue);
        }
    }

    public String addingSingleQuotation(Object originValue) {
        return "'" + originValue.toString() + "'";
    }
}
