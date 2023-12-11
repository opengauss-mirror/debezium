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
import io.debezium.data.Envelope;
import io.debezium.util.Clock;

/**
 * Description: SqlTools class
 *
 * @author douxin
 * @since 2022/10/31
 **/
public class SqlTools {
    private static final Logger LOGGER = LoggerFactory.getLogger(SqlTools.class);
    private static final String JSON_PREFIX = "::jsonb";
    private static final String POINT_POLYGON_PREFIX = "~";
    private static final long ATTEMPTS = 5000L;
    private static final String B_COMPATIBILITY = "B";
    private static final char BACK_QUOTE = '`';
    // default use double quotes to wrap object name, however when database compatibility is B
    // and contains dolphin extension, use back quote to wrap object name
    private static char objectWrappedSymbol = '"';

    private Connection connection;
    private boolean isConnection;

    public SqlTools(Connection connection) {
        this.connection = connection;
        this.isConnection = true;
        getObjectWrapSymbol();
    }

    public TableMetaData getTableMetaData(String schemaName, String tableName) {
        return getTableMetaData(schemaName, tableName, Clock.system().currentTimeInMillis());
    }

    private TableMetaData getTableMetaData(String schemaName, String tableName, long timeMillis) {
        List<ColumnMetaData> columnMetaDataList = new ArrayList<>();
        String sql = String.format(Locale.ENGLISH, "select column_name, data_type, numeric_scale from " +
                "information_schema.columns where table_schema = '%s' and table_name = '%s'" +
                " order by ordinal_position;",
                schemaName, tableName);
        TableMetaData tableMetaData = null;
        try (Statement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery(sql)) {
            while (rs.next()) {
                ColumnMetaData columnMetaData = new ColumnMetaData(rs.getString("column_name"),
                        rs.getString("data_type"), rs.getString("numeric_scale") == null ? -1
                                : rs.getInt("numeric_scale"));
                columnMetaDataList.add(columnMetaData);
            }
            for (int i = 0; i < columnMetaDataList.size(); i++) {
                columnMetaDataList.get(i).setPrimaryColumn(isColumnPrimary(schemaName, tableName, i + 1));
            }
            tableMetaData = new TableMetaData(schemaName, tableName, columnMetaDataList);
        }
        catch (SQLException exp) {
            try {
                if (!connection.isValid(1)) {
                    isConnection = false;
                    return tableMetaData;
                }
            }
            catch (SQLException exception) {
                LOGGER.error("Connection exception occurred");
            }
            LOGGER.error("SQL exception occurred, the sql statement is " + sql);
        }
        long currentTimeMillis = Clock.system().currentTimeInMillis();
        if (columnMetaDataList.isEmpty() && (currentTimeMillis - timeMillis) < ATTEMPTS) {
            LOGGER.info("No data exists in the metadata query result column. columnMetaDataList is {}",
                    columnMetaDataList);
            tableMetaData = null;
            return getTableMetaData(schemaName, tableName, timeMillis);
        }
        return tableMetaData;
    }

    private boolean isColumnPrimary(String schemaName, String tableName, int columnIndex) {
        String[] primaryColumns = getPrimaryKeyValue(schemaName, tableName);
        for (String primaryColumn : primaryColumns) {
            int index = Integer.parseInt(primaryColumn);
            if (index == columnIndex) {
                return true;
            }
        }
        return false;
    }

    /**
     * Gets rely table list
     *
     * @param tableFullName String the table full name
     * @return List<String> the table name list rely on the old table
     */
    public List<String> getForeignTableList(String tableFullName) {
        String sql = String.format(Locale.ENGLISH, "select c.relname, ns.nspname from pg_class c left join"
                + " pg_namespace ns on c.relnamespace=ns.oid left join pg_constraint cons on c.oid=cons.conrelid"
                + " left join pg_class oc on cons.confrelid=oc.oid"
                + " left join  pg_namespace ons on oc.relnamespace=ons.oid"
                + " where oc.relname='%s' and ons.nspname='%s';",
                tableFullName.split("\\.")[1], tableFullName.split("\\.")[0]);
        try (Statement statement = connection.createStatement(); ResultSet rs = statement.executeQuery(sql)) {
            List<String> tableList = new ArrayList<>();
            while (rs.next()) {
                tableList.add(rs.getString("nspname") + "." + rs.getString("relname"));
            }
            return tableList;
        }
        catch (SQLException e) {
            LOGGER.error("SQL exception occurred in sql tools", e);
        }
        return new ArrayList<>(0);
    }

    private String[] getPrimaryKeyValue(String schemaName, String tableName) {
        String sql = String.format(Locale.ENGLISH, "select conkey from pg_constraint where "
                + "conrelid = (select oid from pg_class where relname = '%s' and "
                + "relnamespace = (select oid from pg_namespace where nspname= '%s')) and"
                + " contype = 'p';", tableName, schemaName);
        try (Statement statement = connection.createStatement(); ResultSet rs = statement.executeQuery(sql)) {
            while (rs.next()) {
                String indexes = rs.getString("conkey");
                if (indexes != null) {
                    return indexes.substring(indexes.indexOf("{") + 1, indexes.lastIndexOf("}"))
                            .split(",");
                }
            }
        }
        catch (SQLException e) {
            LOGGER.error("SQL exception occurred in sql tools", e);
        }
        return new String[0];
    }

    /**
     * Get object wrap symbol
     */
    public void getObjectWrapSymbol() {
        String sql_compatibility = "show sql_compatibility";
        String dolphin_extension = "select * from pg_extension where extname = 'dolphin';";
        try (Statement statement1 = connection.createStatement();
                ResultSet rs1 = statement1.executeQuery(sql_compatibility);
                Statement statement2 = connection.createStatement();
                ResultSet rs2 = statement2.executeQuery(dolphin_extension)) {
            while (rs1.next() && rs2.next()) {
                if (B_COMPATIBILITY.equals(rs1.getString(1))) {
                    // when database compatibility is B and contains dolphin extension,
                    // use back quote to wrap object name
                    objectWrappedSymbol = BACK_QUOTE;
                }
            }
        }
        catch (SQLException exp) {
            LOGGER.error("SQL exception occurred in get sql compatibility and dolphin extension", exp);
        }
    }

    /**
     * Adding quote
     *
     * @param String the name
     * @return String the name wrapped by quote
     */
    public static String addingQuote(String name) {
        return objectWrappedSymbol + name + objectWrappedSymbol;
    }

    /**
     * Adding back quote
     *
     * @param String the name
     * @return String the name wrapped by back quote
     */
    public static String addingBackQuote(String name) {
        return BACK_QUOTE + name + BACK_QUOTE;
    }

    /**
     * Remove back quote
     *
     * @param target String the target name
     * @return String the name wrapped by back quote
     */
    public static String removeBackQuote(String target) {
        if (target.charAt(0) == BACK_QUOTE && target.charAt(target.length() - 1) == BACK_QUOTE) {
            return target.substring(1, target.length() - 1);
        }
        return target;
    }

    public String getInsertSql(TableMetaData tableMetaData, Struct after) {
        StringBuilder sb = new StringBuilder();
        sb.append("insert into ").append(tableMetaData.getTableFullName()).append(" values (");
        ArrayList<String> valueList = getValueList(tableMetaData.getColumnList(), after, Envelope.Operation.CREATE);
        sb.append(String.join(", ", valueList));
        sb.append(");");
        return sb.toString();
    }

    public String getUpdateSql(TableMetaData tableMetaData, Struct before, Struct after) {
        List<ColumnMetaData> columnMetaDataList = tableMetaData.getColumnList();
        StringBuilder sb = new StringBuilder();
        sb.append("update ").append(tableMetaData.getTableFullName()).append(" set ");
        ArrayList<String> updateSetValueList = getValueList(tableMetaData.getColumnList(), after,
                Envelope.Operation.UPDATE);
        sb.append(String.join(", ", updateSetValueList));
        sb.append(" where ");
        sb.append(getWhereCondition(tableMetaData, before, Envelope.Operation.DELETE));
        return sb.toString();
    }

    public String getDeleteSql(TableMetaData tableMetaData, Struct before) {
        StringBuilder sb = new StringBuilder();
        sb.append("delete from ").append(tableMetaData.getTableFullName()).append(" where ");
        sb.append(getWhereCondition(tableMetaData, before, Envelope.Operation.DELETE));
        return sb.toString();
    }

    private String getWhereCondition(TableMetaData tableMetaData, Struct before, Envelope.Operation option) {
        ArrayList<String> whereConditionValueList = getWhereConditionList(tableMetaData, before, option);
        StringBuilder sb = new StringBuilder();
        sb.append(String.join(" and ", whereConditionValueList));
        sb.append(";");
        return sb.toString();
    }

    private ArrayList<String> getWhereConditionList(TableMetaData tableMetaData,
                                                    Struct before, Envelope.Operation option) {
        List<ColumnMetaData> primaryColumnMetaDataList = new ArrayList<>();
        for (ColumnMetaData column : tableMetaData.getColumnList()) {
            if (column.isPrimaryColumn()) {
                primaryColumnMetaDataList.add(column);
            }
        }
        ArrayList<String> whereConditionValueList;
        if (primaryColumnMetaDataList.size() > 0) {
            whereConditionValueList = getValueList(primaryColumnMetaDataList, before, option);
        }
        else {
            whereConditionValueList = getValueList(tableMetaData.getColumnList(), before, option);
        }
        return whereConditionValueList;
    }

    private ArrayList<String> getValueList(List<ColumnMetaData> columnMetaDataList, Struct after,
                                           Envelope.Operation operation) {
        ArrayList<String> valueList = new ArrayList<>();
        String singleValue;
        String columnName;
        String columnType;
        for (ColumnMetaData columnMetaData : columnMetaDataList) {
            singleValue = DebeziumValueConverters.getValue(columnMetaData, after);
            columnName = columnMetaData.getWrappedColumnName();
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
                    }
                    else if (columnType.equals("json")) {
                        valueList.add(columnName + JSON_PREFIX + "=" + singleValue);
                    }
                    else if (columnType.equals("point") || columnType.equals("polygon")) {
                        valueList.add(columnName + POINT_POLYGON_PREFIX + "=" + singleValue);
                    }
                    else {
                        valueList.add(columnName + " = " + singleValue);
                    }
                    break;
            }
        }
        return valueList;
    }

    /**
     * Determine whether the sql statement is create or alter table
     *
     * @param String the sql statement
     * @return boolean true if is create or alter table
     */
    public static boolean isCreateOrAlterTableStatement(String sql) {
        return sql.toLowerCase(Locale.ROOT).startsWith("create table") ||
                sql.toLowerCase(Locale.ROOT).startsWith("alter table");
    }

    /**
     * Get xlog position
     *
     * @return String the xlog position
     */
    public String getXlogLocation() {
        String xlogPosition = "";
        try (Statement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery("select pg_current_xlog_location();")) {
            if (rs.next()) {
                xlogPosition = rs.getString(1);
            }
        }
        catch (SQLException exp) {
            LOGGER.error("Fail to get current xlog position.");
        }
        return xlogPosition;
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
        sb.append("select * from ").append(tableMetaData.getTableFullName()).append(" where ");
        List<ColumnMetaData> columnMetaDataList = tableMetaData.getColumnList();
        ArrayList<String> valueList;
        if (Envelope.Operation.CREATE.equals(operation)) {
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
        sb.append("select * from ").append(tableMetaData.getTableFullName()).append(" where ");
        String extraSql = sb.toString();
        ArrayList<String> updateSetValueList = getValueList(tableMetaData.getColumnList(), after,
                Envelope.Operation.UPDATE);
        ArrayList<String> whereConditionList = getWhereConditionList(tableMetaData, before,
                Envelope.Operation.DELETE);
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
        try (
                Statement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery(sql)) {
            if (rs.next()) {
                isExistSql = true;
            }
        }
        catch (SQLException exception) {
            LOGGER.error("SQL exception occurred, the sql statement is " + sql);
        }
        return isExistSql;
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
     * Close the connection
     */
    public void closeConnection() {
        if (connection != null) {
            try {
                connection.close();
            }
            catch (SQLException exp) {
                LOGGER.error("Unexpected error while closing the connection, the exception message is {}",
                        exp.getMessage());
            }
            finally {
                connection = null;
            }
        }
    }
}
