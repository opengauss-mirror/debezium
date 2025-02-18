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

package io.debezium.connector.opengauss.sink.utils;

import io.debezium.connector.opengauss.sink.object.ColumnMetaData;
import io.debezium.connector.opengauss.sink.object.TableMetaData;
import io.debezium.data.Envelope;
import io.debezium.util.Clock;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Description: PostgreSqlTools
 *
 * @author tianbin
 * @since 2024/10/22
 */
public class PostgreSqlTools extends SqlTools {
    private static final Logger LOGGER = LoggerFactory.getLogger(PostgreSqlTools.class);
    private static final long ATTEMPTS = 5000L;
    private static final String JSON_PREFIX = "::jsonb";
    private static final String POINT_POLYGON_PREFIX = "~";
    private static final String PG_COMPATIBILITY = "PG";
    private static char objectWrappedSymbol = '"';
    private static char quote = '"';
    private static char delimiter = '|';
    private static final String LOAD_SQL = "COPY %s FROM STDIN (DELIMITER '%s', "
            + "HEADER false, FORMAT 'csv', NULL '\\N', QUOTE '" + quote + "');";

    private final Connection connection;
    private boolean isConnection;

    /**
     * Constructor
     *
     * @param connection pgConnection
     */
    public PostgreSqlTools(Connection connection) {
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
    @Override
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
            if (Integer.parseInt(primaryColumn) == columnIndex) {
                return true;
            }
        }
        return false;
    }

    private String[] getPrimaryKeyValue(String schemaName, String tableName) {
        String sql = String.format(Locale.ENGLISH,
                "select conkey from pg_constraint where "
                        + "conrelid = (select oid from pg_class where relname = '%s' and "
                        + "relnamespace = (select oid from pg_namespace where nspname= '%s')) and" + " contype = 'p';",
                tableName, schemaName);
        try (Statement statement = connection.createStatement(); ResultSet rs = statement.executeQuery(sql)) {
            while (rs.next()) {
                String indexes = rs.getString("conkey");
                if (indexes != null) {
                    return indexes.substring(indexes.indexOf("{") + 1, indexes.lastIndexOf("}")).split(",");
                }
            }
        } catch (SQLException e) {
            LOGGER.error("SQL exception occurred in sql tools", e);
        }
        return new String[0];
    }

    /**
     * Gets isConnection.
     *
     * @return the value of isConnection
     */
    @Override
    public Boolean getIsConnection() {
        return isConnection;
    }

    /**
     * Get insert sql
     *
     * @param tableMetaData tableMetaData
     * @param after after
     * @return Executable SQL statements
     */
    @Override
    public String getInsertSql(TableMetaData tableMetaData, Struct after) {
        StringBuilder sb = new StringBuilder();
        sb.append("insert into ").append(getTableFullName(tableMetaData)).append(" values (");
        List<String> valueList = getValueList(tableMetaData.getColumnList(), after, Envelope.Operation.CREATE);
        sb.append(String.join(", ", valueList));
        sb.append(");");
        return sb.toString();
    }

    /**
     * Get update sql
     *
     * @param tableMetaData tableMetaData
     * @param after after
     * @param before before
     * @return Executable SQL statements
     */
    @Override
    public String getUpdateSql(TableMetaData tableMetaData, Struct before, Struct after) {
        StringBuilder sb = new StringBuilder();
        sb.append("update ").append(getTableFullName(tableMetaData)).append(" set ");
        List<String> updateSetValueList = getValueList(tableMetaData.getColumnList(), after, Envelope.Operation.UPDATE);
        sb.append(String.join(", ", updateSetValueList));
        sb.append(" where ");
        sb.append(getWhereCondition(tableMetaData, before, Envelope.Operation.DELETE));
        return sb.toString();
    }

    /**
     * Get delete sql
     *
     * @param tableMetaData tableMetaData
     * @param before before
     * @return Executable SQL statements
     */
    @Override
    public String getDeleteSql(TableMetaData tableMetaData, Struct before) {
        StringBuilder sb = new StringBuilder();
        sb.append("delete from ").append(getTableFullName(tableMetaData)).append(" where ");
        sb.append(getWhereCondition(tableMetaData, before, Envelope.Operation.DELETE));
        return sb.toString();
    }

    private String getWhereCondition(TableMetaData tableMetaData, Struct before, Envelope.Operation option) {
        List<String> whereConditionValueList = getWhereConditionList(tableMetaData, before, option);
        StringBuilder sb = new StringBuilder();
        sb.append(String.join(" and ", whereConditionValueList));
        sb.append(";");
        return sb.toString();
    }

    private List<String> getWhereConditionList(TableMetaData tableMetaData, Struct before, Envelope.Operation option) {
        List<ColumnMetaData> primaryColumnMetaDataList = new ArrayList<>();
        for (ColumnMetaData column : tableMetaData.getColumnList()) {
            if (column.isPrimaryKeyColumn()) {
                primaryColumnMetaDataList.add(column);
            }
        }
        List<String> whereConditionValueList;
        if (primaryColumnMetaDataList.size() > 0) {
            whereConditionValueList = getValueList(primaryColumnMetaDataList, before, option);
        } else {
            whereConditionValueList = getValueList(tableMetaData.getColumnList(), before, option);
        }
        return whereConditionValueList;
    }

    private List<String> getValueList(List<ColumnMetaData> columnMetaDataList, Struct after,
            Envelope.Operation operation) {
        List<String> valueList = new ArrayList<>();
        String singleValue;
        String columnName;
        String columnType;
        for (ColumnMetaData columnMetaData : columnMetaDataList) {
            singleValue = PostgresValueConverters.getValue(columnMetaData, after);
            columnName = getWrappedName(columnMetaData.getColumnName());
            columnType = columnMetaData.getColumnType();
            switch (operation) {
                case READ:
                case DELETE:
                    addDeleteValueList(singleValue, valueList, columnName, columnType);
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

    private void addDeleteValueList(String singleValue, List<String> valueList, String columnName, String columnType) {
        if (singleValue == null) {
            valueList.add(columnName + " is null");
        } else if (columnType.equals("json")) {
            valueList.add(columnName + JSON_PREFIX + "=" + singleValue);
        } else if (columnType.equals("point") || columnType.equals("polygon")) {
            valueList.add(columnName + POINT_POLYGON_PREFIX + "=" + singleValue);
        } else {
            valueList.add(columnName + " = " + singleValue);
        }
    }

    /**
     * modifying sql statements
     *
     * @param tableMetaData TableMetaData the table meta data
     * @param columnString sql columnString
     * @param loadSql sql of load data
     * @return load data
     */
    @Override
    public String sqlAddBitCast(TableMetaData tableMetaData, String columnString, String loadSql) {
        return loadSql;
    }

    /**
     * Full data type conversion
     * Full data type conversion
     *
     * @param columnList ColumnMetaData the table meta data
     * @param lineList old data
     * @param after Struct
     * @return new data
     */
    @Override
    public List<String> conversionFullData(List<ColumnMetaData> columnList, List<String> lineList, Struct after) {
        return lineList;
    }

    /**
     * Get read sql
     *
     * @param tableMetaData the tableMetaData
     * @param struct the struct
     * @param operation the operation
     * @return read sql
     */
    @Override
    public String getReadSql(TableMetaData tableMetaData, Struct struct, Envelope.Operation operation) {
        StringBuilder sb = new StringBuilder();
        sb.append("select * from ").append(getTableFullName(tableMetaData)).append(" where ");
        List<String> valueList;
        if (Envelope.Operation.DELETE.equals(operation)) {
            valueList = getWhereConditionList(tableMetaData, struct, Envelope.Operation.READ);
        } else {
            List<ColumnMetaData> columnMetaDataList = tableMetaData.getColumnList();
            valueList = getValueList(columnMetaDataList, struct, Envelope.Operation.READ);
        }
        sb.append(String.join(" and ", valueList));
        sb.append(";");
        return sb.toString();
    }

    /**
     * Is or not exist data
     *
     * @param sql the sql
     * @return exist the data
     */
    @Override
    public boolean isExistSql(String sql) {
        try (Statement statement = connection.createStatement(); ResultSet rs = statement.executeQuery(sql)) {
            if (rs.next()) {
                return true;
            }
        } catch (SQLException exception) {
            LOGGER.error("SQL exception occurred, the sql statement is " + sql);
        }
        return false;
    }

    /**
     * Get read sql for update
     *
     * @param tableMetaData tableMetaData
     * @param before before
     * @param after after
     * @return list sql list
     */
    @Override
    public List<String> getReadSqlForUpdate(TableMetaData tableMetaData, Struct before, Struct after) {
        StringBuilder sb = new StringBuilder();
        sb.append("select * from ").append(getTableFullName(tableMetaData)).append(" where ");
        String extraSql = sb.toString();
        List<String> updateSetValueList = getValueList(tableMetaData.getColumnList(), after, Envelope.Operation.READ);
        List<String> whereConditionList = getWhereConditionList(tableMetaData, before, Envelope.Operation.READ);
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
     * Gets rely table list
     *
     * @param tableFullName the table full name
     * @return List<String> the table name list rely on the old table
     */
    @Override
    public List<String> getForeignTableList(String tableFullName) {
        String sql = String.format(Locale.ENGLISH, "select c.relname, ns.nspname from pg_class c left join"
                + " pg_namespace ns on c.relnamespace=ns.oid left join pg_constraint cons on c.oid=cons.conrelid"
                + " left join pg_class oc on cons.confrelid=oc.oid"
                + " left join  pg_namespace ons on oc.relnamespace=ons.oid"
                + " where oc.relname='%s' and ons.nspname='%s';", tableFullName.split("\\.")[1],
                tableFullName.split("\\.")[0]);
        try (Statement statement = connection.createStatement(); ResultSet rs = statement.executeQuery(sql)) {
            List<String> tableList = new ArrayList<>();
            while (rs.next()) {
                tableList.add(rs.getString("nspname") + "." + rs.getString("relname"));
            }
            return tableList;
        } catch (SQLException e) {
            LOGGER.error("SQL exception occurred in sql tools", e);
        }
        return new ArrayList<>(0);
    }

    /**
     * Get full migrate sql
     *
     * @param tableFullName tableFullName
     * @return Executable SQL statements
     */
    public String loadFullSql(String tableFullName) {
        return String.format(Locale.ROOT, LOAD_SQL, tableFullName, delimiter);
    }

    /**
     * Get wrapped name
     *
     * @param name the name
     * @return String the wrapped name
     */
    @Override
    public String getWrappedName(String name) {
        return objectWrappedSymbol + name + objectWrappedSymbol;
    }
}
