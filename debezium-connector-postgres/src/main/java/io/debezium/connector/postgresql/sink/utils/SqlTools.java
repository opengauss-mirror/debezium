/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package io.debezium.connector.postgresql.sink.utils;

import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import org.apache.kafka.connect.data.Struct;
import org.opengauss.copy.CopyManager;
import org.opengauss.core.BaseConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.postgresql.migration.MigrationUtil;
import io.debezium.connector.postgresql.migration.PostgresSqlConstant;
import io.debezium.connector.postgresql.sink.common.SourceDataField;
import io.debezium.connector.postgresql.sink.object.ColumnMetaData;
import io.debezium.connector.postgresql.sink.object.DataReplayOperation;
import io.debezium.connector.postgresql.sink.object.TableMetaData;
import io.debezium.connector.postgresql.sink.record.SinkDataRecord;
import io.debezium.data.Envelope;
import io.debezium.util.Clock;

/**
 * Description: SqlTools class
 *
 * @author tianbin
 * @since 2024-11-25
 **/
public class SqlTools {
    private static final Logger LOGGER = LoggerFactory.getLogger(SqlTools.class);
    private static final String JSON_PREFIX = "::jsonb";
    private static final String POINT_POLYGON_PREFIX = "~";
    private static final long ATTEMPTS = 5000L;
    private static final String PG_COMPATIBILITY = "PG";
    private static final String COPY_SQL = "COPY %s FROM STDIN WITH NULL 'null' "
            + "CSV QUOTE '\"' DELIMITER ',' ESCAPE '\"'";
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

    /**
     * Get table full name
     *
     * @param tableMetaData the table metadata
     * @return String the table Full name
     */
    public String getTableFullName(TableMetaData tableMetaData) {
        return getWrappedName(tableMetaData.getSchemaName()) + "." + getWrappedName(tableMetaData.getTableName());
    }

    /**
     * Get Table MetaData
     *
     * @param schemaName schemaName
     * @param tableName tableName
     * @return tableMetaData
     */
    public TableMetaData getTableMetaData(String schemaName, String tableName) {
        return getTableMetaData(schemaName, tableName, Clock.system().currentTimeInMillis());
    }

    private TableMetaData getTableMetaData(String schemaName, String tableName, long timeMillis) {
        List<ColumnMetaData> columnMetaDataList = new ArrayList<>();
        String sql = String.format(Locale.ENGLISH, "select column_name, data_type, numeric_scale, "
                + "character_maximum_length from information_schema.columns where table_schema = '%s' "
                + "and table_name = '%s' order by ordinal_position;",
                schemaName, tableName);
        TableMetaData tableMetaData = null;
        try (Statement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery(sql)) {
            while (rs.next()) {
                ColumnMetaData columnMetaData = new ColumnMetaData(rs.getString("column_name"),
                        rs.getString("data_type"), rs.getString("numeric_scale") == null ? -1
                                : rs.getInt("numeric_scale"));
                if (rs.getString("character_maximum_length") != null) {
                    columnMetaData.setLength(rs.getInt("character_maximum_length"));
                }
                columnMetaDataList.add(columnMetaData);
            }
            for (int i = 0; i < columnMetaDataList.size(); i++) {
                columnMetaDataList.get(i).setPrimaryKeyColumn(isColumnPrimary(schemaName, tableName, i + 1));
            }
            tableMetaData = new TableMetaData(schemaName, tableName, columnMetaDataList);
        }
        catch (SQLException exp) {
            try {
                if (!connection.isValid(1)) {
                    isConnection = false;
                    return tableMetaData;
                }
            } catch (SQLException exception) {
                LOGGER.error("Connection exception occurred");
            }
            LOGGER.error("SQL exception occurred, the sql statement is {}.", sql, exp);
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
     * @param tableFullName the table full name
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
                tableList.add(rs.getString("ns.nspname") + "." + rs.getString("c.relname"));
            }
            return tableList;
        } catch (SQLException e) {
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
        String sqlCompatibility = "show sql_compatibility";
        try (Statement statement1 = connection.createStatement();
                ResultSet rs = statement1.executeQuery(sqlCompatibility);) {
            while (rs.next()) {
                if (PG_COMPATIBILITY.equals(rs.getString(1))) {
                    // when database compatibility is B and contains dolphin extension,
                    // use back quote to wrap object name
                    objectWrappedSymbol = '"';
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
     * @param name the name
     * @return String the name wrapped by quote
     */
    public static String addingQuote(String name) {
        return objectWrappedSymbol + name + objectWrappedSymbol;
    }

    /**
     * Adding back quote
     *
     * @param name the name
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

    /**
     * Generate an SQL INSERT statement for the given table metadata and Struct.
     *
     * @param tableMetaData The metadata of the table into which the data is to be inserted.
     * @param after The Struct containing the data to be inserted.
     * @return The SQL INSERT statement as a string.
     */
    public String getInsertSql(TableMetaData tableMetaData, Struct after) {
        StringBuilder sb = new StringBuilder();
        sb.append("insert into ").append(getTableFullName(tableMetaData)).append(" values (");
        ArrayList<String> valueList = getValueList(tableMetaData.getColumnList(), after, Envelope.Operation.CREATE);
        sb.append(String.join(", ", valueList));
        sb.append(");");
        return sb.toString();
    }

    /**
     * Generate an SQL UPDATE statement for the given table metadata, Structs representing the old and new data.
     *
     * @param tableMetaData The metadata of the table to be updated.
     * @param before The Struct containing the old data before the update.
     * @param after The Struct containing the new data after the update.
     * @return The SQL UPDATE statement as a string.
     */
    public String getUpdateSql(TableMetaData tableMetaData, Struct before, Struct after) {
        List<ColumnMetaData> columnMetaDataList = tableMetaData.getColumnList();
        StringBuilder sb = new StringBuilder();
        sb.append("update ").append(getTableFullName(tableMetaData)).append(" set ");
        ArrayList<String> updateSetValueList = getValueList(tableMetaData.getColumnList(), after,
                Envelope.Operation.UPDATE);
        sb.append(String.join(", ", updateSetValueList));
        sb.append(" where ");
        sb.append(getWhereCondition(tableMetaData, before, Envelope.Operation.DELETE));
        return sb.toString();
    }

    /**
     * Generate an SQL DELETE statement for the given table metadata and Struct representing the old data.
     *
     * @param tableMetaData The metadata of the table from which data is to be deleted.
     * @param before The Struct containing the old data before the deletion.
     * @return The SQL DELETE statement as a string.
     */
    public String getDeleteSql(TableMetaData tableMetaData, Struct before) {
        StringBuilder sb = new StringBuilder();
        sb.append("delete from ").append(getTableFullName(tableMetaData)).append(" where ");
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

    private ArrayList<String> getValueList(List<ColumnMetaData> columnMetaDataList, Struct after,
                                           Envelope.Operation operation) {
        ArrayList<String> valueList = new ArrayList<>();
        String singleValue;
        String columnName;
        String columnType;
        for (ColumnMetaData columnMetaData : columnMetaDataList) {
            singleValue = DebeziumValueConverters.getValue(columnMetaData, after);
            columnName = getWrappedName(columnMetaData.getColumnName());
            columnType = columnMetaData.getColumnType();
            switch (operation) {
                case READ:
                case DELETE:
                    if (singleValue == null) {
                        valueList.add(columnName + " is null");
                    } else if (columnType.equals("json")) {
                        valueList.add(columnName + JSON_PREFIX + "=" + singleValue);
                    } else if (columnType.equals("point") || columnType.equals("polygon")) {
                        valueList.add(columnName + POINT_POLYGON_PREFIX + "=" + singleValue);
                    } else {
                        valueList.add(columnName + " = " + singleValue);
                    }
                    break;
                case CREATE:
                    valueList.add(singleValue);
                    break;
                case UPDATE:
                    valueList.add(columnName + " = " + singleValue);
                    break;
            }
        }
        return valueList;
    }

    /**
     * Determine whether the sql statement is create or alter table
     *
     * @param sql the sql statement
     * @return boolean true if is create or alter table
     */
    public static boolean isCreateOrAlterTableStatement(String sql) {
        return sql.toLowerCase(Locale.ROOT).startsWith("create table")
                || sql.toLowerCase(Locale.ROOT).startsWith("alter table");
    }

    /**
     * Get xlog position
     *
     * @return String the xlog position
     */
    public String getXlogLocation() {
        String xlogPosition = "";
        boolean isLessThanVersion10 = ConnectionUtil.isServerVersionLessThan(connection,
                PostgresSqlConstant.PG_SERVER_V10);
        String query = isLessThanVersion10 ? PostgresSqlConstant.GET_XLOG_LOCATION_OLD
                : PostgresSqlConstant.GET_XLOG_LOCATION_NEW;
        try (Statement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery(query)) {
            if (rs.next()) {
                xlogPosition = rs.getString(1);
            }
        } catch (SQLException exp) {
            LOGGER.error("Fail to get current xlog position.", exp);
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
                Envelope.Operation.READ);
        ArrayList<String> whereConditionList = getWhereConditionList(tableMetaData, before,
                Envelope.Operation.READ);
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
        } catch (SQLException exception) {
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
            } catch (SQLException exp) {
                LOGGER.error("Unexpected error while closing the connection, the exception message is {}",
                        exp.getMessage());
            } finally {
                connection = null;
            }
        }
    }

    /**
     * Get wrapped column name
     *
     * @paran name the name
     * @return String the wrapped name
     */
    public String getWrappedName(String name) {
        return objectWrappedSymbol + name + objectWrappedSymbol;
    }

    /**
     * Process full data from a CSV file and insert it into a specified table.
     *
     * @param path The path to the CSV file containing the data to be processed.
     * @param tableFullName The full name of the table (including schema) where the data will be inserted.
     * @param connection The database connection to use for the operation.
     * @return The number of rows inserted into the table.
     * @throws SQLException If a database access error occurs.
     * @throws IOException If an I/O error occurs while reading the CSV file.
     */
    public long processFullData(String path, String tableFullName, Connection connection)
            throws SQLException, IOException {
        CopyManager copyManager = new CopyManager((BaseConnection) connection);
        FileReader csvReader = new FileReader(path);
        return copyManager.copyIn(String.format(Locale.ROOT, COPY_SQL, tableFullName), csvReader);
    }

    /**
     * Replays the index operation for a given data replay operation and sink data record.
     * This method is responsible for switching the schema, modifying the index DDL, and executing it.
     *
     * @param dataReplayOperation The data replay operation containing the index DDL.
     * @param sinkDataRecord The sink data record containing the source field information.
     * @param sinkSchema The target schema where the index will be created.
     */
    public void replayIndex(DataReplayOperation dataReplayOperation, SinkDataRecord sinkDataRecord,
                            String sinkSchema) {
        SourceDataField source = sinkDataRecord.getSourceField();
        String sourceFullName = source.getSchema() + "." + source.getTable();
        String sinkFullName = sinkSchema + "." + source.getTable();
        MigrationUtil.switchSchema(sinkSchema, connection);
        String idxDdl = dataReplayOperation.getIndex();
        idxDdl = idxDdl.replace(sourceFullName, sinkFullName);
        try (Statement stmt = connection.createStatement()) {
            LOGGER.info("start build index for table {}, ddl: {}", sinkFullName, idxDdl);
            stmt.execute(idxDdl);
            LOGGER.info("finish build index for table{}, ddl: {}", sinkFullName, idxDdl);
        } catch (SQLException e) {
            LOGGER.error("create index occurred SQLException", e);
        }
    }

    /**
     * Generates an SQL INSERT statement for the given sink data record.
     * This method is responsible for constructing the SQL statement to insert a record into the replica tables.
     *
     * @param sinkDataRecord The sink data record containing the source field information.
     * @return The SQL INSERT statement as a string.
     */
    public String getTableRecordSnapshotSql(SinkDataRecord sinkDataRecord) {
        SourceDataField sourceField = sinkDataRecord.getSourceField();
        String schema = sourceField.getSchema();
        String table = sourceField.getTable();
        DataReplayOperation dataReplayOperation = sinkDataRecord.getDataReplayOperation();
        String xlogLocation = dataReplayOperation.getSnapshot();
        return String.format(PostgresSqlConstant.INSERT_REPLICA_TABLES_SQL, schema, table, xlogLocation, xlogLocation);
    }
}
