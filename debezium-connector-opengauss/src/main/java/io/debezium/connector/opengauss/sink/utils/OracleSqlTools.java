/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.opengauss.sink.utils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.opengauss.sink.object.ColumnMetaData;
import io.debezium.connector.opengauss.sink.object.TableMetaData;
import io.debezium.data.Envelope;

/**
 * Description: OracleSqlTools class
 *
 * @author liukaikai
 * @since 2023/11/27
 **/
public class OracleSqlTools extends SqlTools {
    private static final Logger LOGGER = LoggerFactory.getLogger(OracleSqlTools.class);
    private Connection connection;
    private boolean isConnection;

    public OracleSqlTools(Connection connection) {
        this.connection = connection;
        this.isConnection = true;
    }

    @Override
    public TableMetaData getTableMetaData(String schemaName, String tableName) {
        List<ColumnMetaData> columnMetaDataList = new ArrayList<>();
        String sql = String.format(Locale.ENGLISH, "select column_name, data_type from "
                + "ALL_TAB_COLUMNS where owner = '%s' and table_name = '%s'"
                + " order by column_id",
                schemaName.toUpperCase(Locale.ROOT), tableName.toUpperCase(Locale.ROOT));
        TableMetaData tableMetaData = null;
        try (Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery(sql)) {
            while (rs.next()) {
                columnMetaDataList.add(new ColumnMetaData(rs.getString("column_name").toLowerCase(Locale.ROOT),
                        rs.getString("data_type")));
            }
            tableMetaData = new TableMetaData(schemaName, tableName, columnMetaDataList);
        } catch (SQLException exp) {
            LOGGER.error("SQL exception occurred, the sql statement is " + sql, exp);
        }
        return tableMetaData;
    }

    @Override
    public Boolean getIsConnection() {
        return this.isConnection;
    }

    @Override
    public String getInsertSql(TableMetaData tableMetaData, Struct after) {
        StringBuilder sb = new StringBuilder();
        sb.append("insert into ").append(getTableFullName(tableMetaData)).append(" values(");
        ArrayList<String> valueList = getValueList(tableMetaData.getColumnList(), after, Envelope.Operation.CREATE);
        sb.append(String.join(", ", valueList));
        sb.append(")");
        return sb.toString();
    }

    private ArrayList<String> getValueList(List<ColumnMetaData> columnMetaDataList, Struct after,
                                           Envelope.Operation operation) {
        ArrayList<String> valueList = new ArrayList<>();
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

    @Override
    public String getUpdateSql(TableMetaData tableMetaData, Struct before, Struct after) {
        StringBuilder sb = new StringBuilder();
        sb.append("update ").append(getTableFullName(tableMetaData)).append(" set ");
        ArrayList<String> updateSetValueList = getValueList(tableMetaData.getColumnList(), after,
                Envelope.Operation.UPDATE);
        sb.append(String.join(", ", updateSetValueList));
        sb.append(" where ");
        sb.append(getWhereCondition(tableMetaData, before, Envelope.Operation.DELETE));
        return sb.toString();
    }

    private String getWhereCondition(TableMetaData tableMetaData, Struct before, Envelope.Operation option) {
        ArrayList<String> whereConditionValueList = getWhereConditionList(tableMetaData, before, option);
        StringBuilder sb = new StringBuilder();
        sb.append(String.join(" and ", whereConditionValueList));
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

    @Override
    public String getDeleteSql(TableMetaData tableMetaData, Struct before) {
        StringBuilder sb = new StringBuilder();
        sb.append("delete from ").append(getTableFullName(tableMetaData)).append(" where ");
        sb.append(getWhereCondition(tableMetaData, before, Envelope.Operation.DELETE));
        return sb.toString();
    }

    @Override
    public String sqlAddBitCast(TableMetaData tableMetaData, String columnString, String loadSql) {
        return null;
    }

    @Override
    public List<String> conversionFullData(List<ColumnMetaData> columnList, List<String> lineList, Struct after) {
        return null;
    }

    @Override
    public String getReadSql(TableMetaData tableMetaData, Struct struct, Envelope.Operation operation) {
        StringBuilder sb = new StringBuilder();
        sb.append("select * from ").append(getTableFullName(tableMetaData)).append(" where ");
        List<ColumnMetaData> columnMetaDataList = tableMetaData.getColumnList();
        ArrayList<String> valueList = getValueList(columnMetaDataList, struct, operation);
        sb.append(String.join(" and ", valueList));
        sb.append(";");
        return sb.toString();
    }

    @Override
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

    @Override
    public List<String> getReadSqlForUpdate(TableMetaData tableMetaData, Struct before, Struct after) {
        StringBuilder sb = new StringBuilder();
        sb.append("select * from ").append(getTableFullName(tableMetaData)).append(" where ");
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

    @Override
    public List<String> getForeignTableList(String tableFullName) {
        String sql = String.format(Locale.ENGLISH, "select TABLE_NAME from all_cons_columns"
                        + "  where table_name='%s' and owner='%s'",
                tableFullName.split("\\.")[1], tableFullName.split("\\.")[0]);
        try (PreparedStatement preparedStatement = connection.prepareStatement(sql);
             ResultSet resultSet = preparedStatement.executeQuery();) {
            List<String> tableList = new ArrayList<>();
            while (resultSet.next()) {
                tableList.add(tableFullName.split("\\.")[0] + "." + resultSet.getString("TABLE_NAME"));
            }
            return tableList;
        } catch (SQLException e) {
            LOGGER.error("SQL exception occurred in sql tools", e);
        }
        return null;
    }

    @Override
    public String getWrappedName(String name) {
        return name;
    }
}
