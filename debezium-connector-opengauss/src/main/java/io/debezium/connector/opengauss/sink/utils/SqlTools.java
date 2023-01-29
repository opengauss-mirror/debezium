/**
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.opengauss.sink.utils;

import io.debezium.connector.opengauss.sink.object.ColumnMetaData;
import io.debezium.connector.opengauss.sink.object.TableMetaData;
import io.debezium.data.Envelope;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Locale;

/**
 * Description: SqlTools class
 * @author wangzhengyuan
 * @date 2022/12/01
 */
public class SqlTools {
    private static final Logger LOGGER = LoggerFactory.getLogger(SqlTools.class);
    private Connection connection;
    private static Map<String, String> primeKeyTableMap = new HashMap<>();

    /**
     * Gets connection
     *
     * @return Connection the connection
     */
    public Connection getConnection() {
        return connection;
    }

    /**
     * Constructor
     *
     * @return Connection the connection
     */
    public SqlTools(Connection connection) {
        this.connection = connection;
    }

    /**
     * Gets table meta data
     *
     * @param schemaName String the schema name
     * @param tableName String the table name
     * @return TableMetaData the tableMetaData
     */
    public  TableMetaData getTableMetaData(String schemaName, String tableName) {
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

    private String getPrimaryKeyValue(String schemaName, String tableName) {
        String sql = String.format(Locale.ENGLISH, "SELECT cu.Column_Name FROM  " +
                "INFORMATION_SCHEMA.`KEY_COLUMN_USAGE` cu  WHERE CONSTRAINT_NAME = 'PRIMARY' AND" +
                " cu.Table_Name = '%s' AND CONSTRAINT_SCHEMA='%s';", tableName, schemaName);
        try(PreparedStatement preparedStatement = connection.prepareStatement(sql);
            ResultSet resultSet=preparedStatement.executeQuery(); ){
            while (resultSet.next()){
                return resultSet.getString("Column_Name");
            }
        } catch (SQLException e){
            LOGGER.error("SQL exception occurred in sql tools", e);
        }
        return null;
    }

    /**
     * Gets rely table list
     *
     * @param oldTableName String the old table name
     * @return List<String> the table name list rely on the old table
     */
    public  List<String> getRelyTableList(String oldTableName){
        String sql = String.format(Locale.ENGLISH, "select TABLE_NAME from INFORMATION_SCHEMA.KEY_COLUMN_USAGE" +
                "  where REFERENCED_TABLE_NAME='%s'", oldTableName);
        try(PreparedStatement preparedStatement = connection.prepareStatement(sql);
            ResultSet resultSet=preparedStatement.executeQuery(); ){
            List<String> tableList = new ArrayList<>();
            while (resultSet.next()){
                tableList.add(resultSet.getString("TABLE_NAME"));
            }
            return tableList;
        } catch (SQLException e){
            LOGGER.error("SQL exception occurred in sql tools", e);
        }
        return null;
    }

    /**
     * Gets insert sql
     *
     * @param tableMetaData TableMetaData the table meta data
     * @param after Struct the after
     * @return String the insert sql
     */
    public String getInsertSql(TableMetaData tableMetaData, Struct after){
        List<ColumnMetaData> columnMetaDataList = tableMetaData.getColumnList();
        StringBuilder sb = new StringBuilder();
        sb.append("insert into ").append(tableMetaData.getTableName()).append(" values(");
        ArrayList<String> valueList = getValueList(tableMetaData, after, Envelope.Operation.CREATE);
        sb.append(String.join(", ", valueList));
        sb.append(");");
        return sb.toString();
    }

    /**
     * Gets update sql
     *
     * @param tableMetaData TableMetaData the table meta data
     * @param before Struct the before
     * @param after Struct the after
     * @return String the update sql
     */
    public String getUpdateSql(TableMetaData tableMetaData, Struct before, Struct after) {
        List<ColumnMetaData> columnMetaDataList = tableMetaData.getColumnList();
        StringBuilder sb = new StringBuilder();
        sb.append("update ").append(tableMetaData.getSchemaName()).append(".")
                .append(tableMetaData.getTableName()).append(" set ");
        ArrayList<String> updateSetValueList = getValueList(tableMetaData, after, Envelope.Operation.UPDATE);
        sb.append(String.join(", ", updateSetValueList));
        sb.append(" where ");
        return sb + getWhereCondition(tableMetaData, before, columnMetaDataList);
    }

    /**
     * Gets delete sql
     *
     * @param tableMetaData TableMetaData the table meta data
     * @param before Struct the before
     * @return String the delete sql
     */
    public String getDeleteSql(TableMetaData tableMetaData, Struct before) {
        List<ColumnMetaData> columnMetaDataList = tableMetaData.getColumnList();
        StringBuilder sb = new StringBuilder();
        sb.append("delete from ").append(tableMetaData.getSchemaName()).append(".")
                .append(tableMetaData.getTableName()).append(" where ");
        return sb + getWhereCondition(tableMetaData, before, columnMetaDataList);
    }

    private String getWhereCondition(TableMetaData tableMetaData, Struct before, List<ColumnMetaData> columnMetaDataList) {
        StringBuilder sb = new StringBuilder();
        String primaryKeyColumnName;
        if (primeKeyTableMap.containsKey(tableMetaData.getTableName())){
            primaryKeyColumnName = primeKeyTableMap.get(tableMetaData.getTableName());
        } else {
            primaryKeyColumnName = getPrimaryKeyValue(tableMetaData.getSchemaName(), tableMetaData.getTableName());
            if (primaryKeyColumnName != null){
                primeKeyTableMap.put(tableMetaData.getTableName(), primaryKeyColumnName);
            }
        }
        if (primaryKeyColumnName != null){
            for (ColumnMetaData columnMetaData : columnMetaDataList){
                if (primaryKeyColumnName.equals(columnMetaData.getColumnName())){
                    String value = getValue(columnMetaData, before);
                    sb.append(primaryKeyColumnName).append(" = ").append(value);
                    break;
                }
            }
        } else {
            ArrayList<String> whereConditionValueList = getValueList(tableMetaData, before, Envelope.Operation.DELETE);
            sb.append(String.join(" and ", whereConditionValueList));
        }
        sb.append(";");
        return sb.toString();
    }

    private String getValue(ColumnMetaData columnMetaData, Struct valueStruct){
        String columnType = columnMetaData.getColumnType();
        String columnName = columnMetaData.getColumnName();
        Object object;
        switch (columnType) {
            case "integer":
            case "tinyint":
            case "double":
            case "float":
                return ValueConverter.convertNumberType(columnName, valueStruct);
            case "blob":
            case "tinyblob":
            case "mediumblob":
            case "longblob":
                return ValueConverter.convertBlob(columnName, valueStruct);
            case "datetime":
            case "timestamp":
                return ValueConverter.convertDatetimeAndTimestamp(columnName, valueStruct);
            case "date":
                return ValueConverter.convertDate(columnName, valueStruct);
            case "time":
                return ValueConverter.convertTime(columnName, valueStruct);
            case "binary":
            case "varbinary":
                return ValueConverter.convertBinary(columnName, valueStruct);
            default:
                object = valueStruct.get(columnName);
                return object == null ? null : ValueConverter.addingSingleQuotation(object.toString());
        }
    }

    private ArrayList<String> getValueList(TableMetaData tableMetaData, Struct after, Envelope.Operation operation) {
        ArrayList<String> valueList = new ArrayList<>();
        List<ColumnMetaData> columnMetaDataList = tableMetaData.getColumnList();
        String singleValue;
        String columnName;
        for (ColumnMetaData columnMetaData : columnMetaDataList) {
            singleValue = getValue(columnMetaData, after);
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
