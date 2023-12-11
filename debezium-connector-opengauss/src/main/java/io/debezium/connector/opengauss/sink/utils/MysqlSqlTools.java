/**
 * Copyright Debezium Authors.
 * <p>
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.opengauss.sink.utils;

import io.debezium.connector.opengauss.sink.object.ColumnMetaData;
import io.debezium.connector.opengauss.sink.object.ConnectionInfo;
import io.debezium.connector.opengauss.sink.object.TableMetaData;
import io.debezium.data.Envelope;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

/**
 * Description: SqlTools class
 * @author wangzhengyuan
 * @date 2022/12/01
 */
public class MysqlSqlTools extends SqlTools {
    private static final Logger LOGGER = LoggerFactory.getLogger(MysqlSqlTools.class);
    private Connection connection;
    private boolean isConnection;
    private List<String> binaryTypes = Arrays.asList("tinyblob", "mediumblob", "longblob", "binary", "varbinary");

    /**
     * Constructor
     *
     * @return Connection the connection
     */
    public MysqlSqlTools(Connection connection) {
        this.connection = connection;
        this.isConnection = true;
    }

    /**
     * Gets table meta data
     *
     * @param schemaName String the schema name
     * @param tableName String the table name
     * @return TableMetaData the tableMetaData
     */
    public TableMetaData getTableMetaData(String schemaName, String tableName) {
        List<ColumnMetaData> columnMetaDataList = new ArrayList<>();
        String sql = String.format(Locale.ENGLISH, "select column_name, data_type, column_key from " +
                        "information_schema.columns where table_schema = '%s' and table_name = '%s'" +
                        " order by ordinal_position;",
                schemaName, tableName);
        TableMetaData tableMetaData = null;
        try (Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery(sql)) {
            while (rs.next()) {
                columnMetaDataList.add(new ColumnMetaData(rs.getString("column_name"),
                        rs.getString("data_type"), "PRI".equals(rs.getString("column_key"))));
            }
            tableMetaData = new TableMetaData(schemaName, tableName, columnMetaDataList);
        } catch (SQLException exp) {
            try {
                if (!connection.isValid(1)) {
                    isConnection = false;
                    return tableMetaData;
                }
            } catch (SQLException exception) {
                LOGGER.error("Connection exception occurred");
            }
            LOGGER.error("SQL exception occurred, the sql statement is " + sql);
        }
        return tableMetaData;
    }

    /**
     * Gets rely table list
     *
     * @param tableFullName String the table full name
     * @return List<String> the table name list rely on the old table
     */
    public List<String> getForeignTableList(String tableFullName) {
        String sql = String.format(Locale.ENGLISH, "select TABLE_NAME, TABLE_SCHEMA from INFORMATION_SCHEMA"
                + ".KEY_COLUMN_USAGE where REFERENCED_TABLE_NAME='%s' and TABLE_SCHEMA='%s'", tableFullName
                .split("\\.")[1], tableFullName.split("\\.")[0]);
        try (PreparedStatement preparedStatement = connection.prepareStatement(sql);
             ResultSet resultSet = preparedStatement.executeQuery();) {
            List<String> tableList = new ArrayList<>();
            while (resultSet.next()) {
                tableList.add(resultSet.getString("TABLE_SCHEMA") + "." + resultSet.getString("TABLE_NAME"));
            }
            return tableList;
        } catch (SQLException e) {
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
    public String getInsertSql(TableMetaData tableMetaData, Struct after) {
        StringBuilder sb = new StringBuilder();
        sb.append("insert into ").append(getTableFullName(tableMetaData)).append(" values(");
        ArrayList<String> valueList = getValueList(tableMetaData.getColumnList(), after, Envelope.Operation.CREATE);
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
        StringBuilder sb = new StringBuilder();
        sb.append("update ").append(getTableFullName(tableMetaData)).append(" set ");
        ArrayList<String> updateSetValueList = getValueList(tableMetaData.getColumnList(), after,
                Envelope.Operation.UPDATE);
        sb.append(String.join(", ", updateSetValueList));
        sb.append(" where ");
        return sb + getWhereCondition(tableMetaData, before, Envelope.Operation.DELETE);
    }

    /**
     * Gets delete sql
     *
     * @param tableMetaData TableMetaData the table meta data
     * @param before Struct the before
     * @return String the delete sql
     */
    public String getDeleteSql(TableMetaData tableMetaData, Struct before) {
        StringBuilder sb = new StringBuilder();
        sb.append("delete from ").append(getTableFullName(tableMetaData)).append(" where ");
        return sb + getWhereCondition(tableMetaData, before, Envelope.Operation.DELETE);
    }

    /**
     * Full data type conversion
     * Full data type conversion
     *
     * @param columnList ColumnMetaData the table meta data
     * @param data old data
     * @param after Struct
     * @return new data
     */
    public List<String> conversionFullData(List<ColumnMetaData> columnList, List<String> data, Struct after) {
        List<String> result = new ArrayList<>();
        for (String datum : data) {
            StringBuilder sb = new StringBuilder();
            String[] colDatas = datum.split(" \\| ");
            for (int i = 0; i < colDatas.length; i++) {
                String colData = colDatas[i];
                ColumnMetaData columnMetaData = columnList.get(i);
                String value = FullDataConverters.getValue(columnMetaData, colData, after);
                sb.append(value);
                if (i != colDatas.length - 1) {
                    sb.append(",");
                }
            }
            result.add(sb.toString());
        }
        return result;
    }

    /**
     * Gets isConnection.
     *
     * @return the value of isConnection
     */
    public Boolean getIsConnection() {
        return isConnection;
    }

    /**
     * modifying sql statements
     *
     * @param tableMetaData TableMetaData the table meta data
     * @param columnString sql columnString
     * @param sql sql of load data
     * @return load data
     */
    public String sqlAddBitCast(TableMetaData tableMetaData, String columnString, String sql) {
        List<ColumnMetaData> columnList = tableMetaData.getColumnList();
        StringBuilder condition = new StringBuilder(" set");
        boolean hasSpecialType = false;
        String column = columnString;
        String query = sql;
        for (ColumnMetaData columnMetaData : columnList) {
            if ("bit".equals(columnMetaData.getColumnType())) {
                hasSpecialType = true;
                column = column.replace(columnMetaData.getColumnName(),
                        "@" + columnMetaData.getColumnName());
                condition.append(String.format(Locale.ROOT, " %s=cast(@%s as signed)",
                        columnMetaData.getColumnName(), columnMetaData.getColumnName()));
                condition.append(",");
            }
            if (binaryTypes.contains(columnMetaData.getColumnType())) {
                hasSpecialType = true;
                column = column.replace(columnMetaData.getColumnName(),
                        "@" + columnMetaData.getColumnName());
                condition.append(String.format(Locale.ROOT, " %s=UNHEX(@%s)",
                        columnMetaData.getColumnName(), columnMetaData.getColumnName()));
                condition.append(",");
            }
        }
        query = query + "(" + column + ")";
        if (hasSpecialType) {
            condition.deleteCharAt(condition.length() - 1);
            return query + condition.toString();
        }
        return query;
    }

    private String getWhereCondition(TableMetaData tableMetaData, Struct before, Envelope.Operation option) {
        ArrayList<String> whereConditionValueList = getWhereConditionList(tableMetaData, before, option);
        StringBuilder sb = new StringBuilder();
        sb.append(String.join(" and ", whereConditionValueList));
        sb.append(";");
        return sb.toString();
    }

    private ArrayList<String> getWhereConditionList(TableMetaData tableMetaData, Struct before,
                                                    Envelope.Operation option) {
        List<ColumnMetaData> primaryColumnMetaDataList = new ArrayList<>();
        for (ColumnMetaData column : tableMetaData.getColumnList()) {
            if (column.isPrimaryKeyColumn()) {
                primaryColumnMetaDataList.add(column);
            }
        }
        ArrayList<String> whereConditionValueList;
        if (primaryColumnMetaDataList.size() > 0) {
            whereConditionValueList = getValueList(primaryColumnMetaDataList, before, option);
        } else {
            whereConditionValueList = getValueList(tableMetaData.getColumnList(), before, option);
        }
        return whereConditionValueList;
    }

    private ArrayList<String> getValueList(List<ColumnMetaData> columnMetaDataList, Struct after, Envelope.Operation operation) {
        ArrayList<String> valueList = new ArrayList<>();
        String singleValue;
        String columnName;
        String columnType;
        for (ColumnMetaData columnMetaData : columnMetaDataList) {
            singleValue = DebeziumValueConverters.getValue(columnMetaData, after);
            columnName = getWrappedName(columnMetaData.getColumnName());
            columnType = columnMetaData.getColumnType();
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
                    } else if ("json".equals(columnType)) {
                        valueList.add(columnName + "= CAST(" + singleValue + " AS json)");
                    } else {
                        valueList.add(columnName + " = " + singleValue);
                    }
                    break;
            }
        }
        return valueList;
    }

    /**
     * Get read sql
     *
     * @param tableMetaData the tableMetaData
     * @param struct the struct
     * @param operation the operation
     * @return read sql
     */
    public String getReadSql(TableMetaData tableMetaData, Struct struct, Envelope.Operation operation) {
        StringBuilder sb = new StringBuilder();
        sb.append("select * from ").append(getTableFullName(tableMetaData)).append(" where ");
        List<ColumnMetaData> columnMetaDataList = tableMetaData.getColumnList();
        ArrayList<String> valueList;
        if (operation.equals(Envelope.Operation.CREATE)) {
            valueList = getValueList(columnMetaDataList, struct, Envelope.Operation.UPDATE);
        } else {
            valueList = getValueList(columnMetaDataList, struct, operation);
        }
        sb.append(String.join(" and ", valueList));
        sb.append(";");
        return sb.toString();
    }

    /**
     * Get read sql for update
     *
     * @param tableMetaData tableMetaData
     * @param before before
     * @param after after
     * @return list sql list
     */
    public List<String> getReadSqlForUpdate(TableMetaData tableMetaData, Struct before, Struct after) {
        StringBuilder sb = new StringBuilder();
        sb.append("select * from ").append(getTableFullName(tableMetaData)).append(" where ");
        String extraSql = sb.toString();
        ArrayList<String> updateSetValueList = getValueList(tableMetaData.getColumnList(), after,
                Envelope.Operation.UPDATE);
        ArrayList<String> whereConditionList = getWhereConditionList(tableMetaData, before, Envelope.Operation.DELETE);
        List<String> sqlList = new ArrayList<>();
        sb.append(String.join(" and ", updateSetValueList));
        sb.append(";");
        sqlList.add(sb.toString());
        if (updateSetValueList.size() == whereConditionList.size()) {
            extraSql = extraSql + String.join(" and ", whereConditionList) + ";";
            sqlList.add(extraSql);
        }
        return sqlList;
    }

    /**
     * Is or not exist data
     *
     * @param sql the sql
     * @return exist the data
     */
    public boolean isExistSql(String sql) {
        boolean isExistSql = false;
        try (Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery(sql)) {
            if (rs.next()) {
                isExistSql = true;
            }
        } catch (SQLException exception) {
            LOGGER.error("SQL exception occurred, the sql statement is " + sql);
        }
        return isExistSql;
    }
}