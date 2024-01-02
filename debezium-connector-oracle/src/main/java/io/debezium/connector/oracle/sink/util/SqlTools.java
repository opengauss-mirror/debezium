/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.sink.util;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.oracle.sink.object.ColumnMetaData;
import io.debezium.connector.oracle.sink.object.TableChangesField;
import io.debezium.connector.oracle.sink.object.TableMetaData;
import io.debezium.data.Envelope;
import io.debezium.util.Clock;

/**
 * Description: SqlTools class
 *
 * @author gbase
 * @since 2023/07/28
 **/
public class SqlTools {
    private static final Logger LOGGER = LoggerFactory.getLogger(SqlTools.class);
    private static final String JSON_PREFIX = "::jsonb";
    private static final long ATTEMPTS = 5000L;
    private static final String B_COMPATIBILITY = "B";
    private static final char BACK_QUOTE = '`';

    // default use double quotes to wrap object name, however when database compatibility is B
    // and contains dolphin extension, use back quote to wrap object name
    private static char objectWrappedSymbol = '"';

    private Connection connection;

    private final Map<String, String> schemaMappingMap;

    /**
     * SqlTools
     *
     * @param connection the connection
     * @param schemaMappingMap the schema mapping map
     */
    public SqlTools(Connection connection, Map<String, String> schemaMappingMap) {
        this.connection = connection;
        this.schemaMappingMap = schemaMappingMap;
        getObjectWrapSymbol();
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
     * Get object wrap symbol
     */
    public void getObjectWrapSymbol() {
        String sqlCompatibility = "show sql_compatibility";
        String dolphinExtension = "select * from pg_extension where extname = 'dolphin';";
        try (Statement statement1 = connection.createStatement();
                ResultSet rs1 = statement1.executeQuery(sqlCompatibility);
                Statement statement2 = connection.createStatement();
                ResultSet rs2 = statement2.executeQuery(dolphinExtension)) {
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
     * Get table metadata
     *
     * @param schemaName the schema name
     * @param tableName the table name
     * @return TableMetaData
     */
    public TableMetaData getTableMetaData(String schemaName, String tableName) {
        return getTableMetaData(schemaName, tableName, Clock.system().currentTimeInMillis());
    }

    private TableMetaData getTableMetaData(String schemaName, String tableName, long timeMillis) {
        List<ColumnMetaData> columnMetaDataList = new ArrayList<>();
        String sql = String.format(Locale.ENGLISH, "select column_name, data_type, numeric_scale, " +
                "interval_type from information_schema.columns where table_schema = '%s' and table_name = '%s'" +
                " order by ordinal_position;",
                schemaName, tableName);
        TableMetaData tableMetaData = null;
        try (Statement statement = connection.createStatement(); ResultSet rs = statement.executeQuery(sql)) {
            while (rs.next()) {
                ColumnMetaData columnMetaData = new ColumnMetaData(rs.getString("column_name"),
                        rs.getString("data_type"), rs.getString("numeric_scale") == null ? -1
                                : rs.getInt("numeric_scale"),
                        rs.getString("interval_type"));
                columnMetaDataList.add(columnMetaData);
            }
            for (int i = 0; i < columnMetaDataList.size(); i++) {
                columnMetaDataList.get(i).setPrimaryColumn(isColumnPrimary(schemaName, tableName, i + 1));
            }
            tableMetaData = new TableMetaData(schemaName, tableName, columnMetaDataList);
        }
        catch (SQLException exp) {
            LOGGER.error("SQL exception occurred, the sql statement is " + sql);
        }
        long currentTimeMillis = Clock.system().currentTimeInMillis();
        if (columnMetaDataList.isEmpty() && (currentTimeMillis - timeMillis) < ATTEMPTS) {
            LOGGER.info("No data exists in the metadata query result column, schema:{}, table:{}",
                    schemaName, tableName);
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
     * @param oldTableName String the old table name
     * @param schemaName String the schema name
     * @return List<String> the table name list rely on the old table
     */
    public List<String> getRelyTableList(String oldTableName, String schemaName) {
        String sql = String.format(Locale.ENGLISH, "select c.relname, ns.nspname from pg_class c left join"
                + " pg_namespace ns on c.relnamespace=ns.oid left join pg_constraint cons on c.oid=cons.conrelid"
                + " left join pg_class oc on cons.confrelid=oc.oid"
                + " left join  pg_namespace ons on oc.relnamespace=ons.oid"
                + " where oc.relname='%s' and ons.nspname='%s';",
                oldTableName, schemaName);
        try (Statement statement = connection.createStatement(); ResultSet rs = statement.executeQuery(sql)) {
            List<String> tableList = new ArrayList<>();
            while (rs.next()) {
                tableList.add(rs.getString("ns.nspname") + "." + rs.getString("c.relname"));
            }
            return tableList;
        }
        catch (SQLException e) {
            LOGGER.error("SQL exception occurred in sql tools", e);
        }
        return Collections.emptyList();
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
     * Get insert sql
     *
     * @param tableMetaData the table metadata
     * @param after the after struct
     * @return String
     */
    public String getInsertSql(TableMetaData tableMetaData, Struct after) {
        StringBuilder sb = new StringBuilder();
        sb.append("insert into ").append(tableMetaData.getTableFullName()).append(" values (");
        ArrayList<String> valueList = getValueList(tableMetaData.getColumnList(), after, Envelope.Operation.CREATE);
        sb.append(String.join(", ", valueList));
        sb.append(");");
        return sb.toString();
    }

    /**
     * Get update sql
     *
     * @param tableMetaData the table metadata
     * @param before the before struct
     * @param after the after struct
     * @return String
     */
    public String getUpdateSql(TableMetaData tableMetaData, Struct before, Struct after) {
        StringBuilder sb = new StringBuilder();
        sb.append("update ").append(tableMetaData.getTableFullName()).append(" set ");
        ArrayList<String> updateSetValueList = getValueList(tableMetaData.getColumnList(), after,
                Envelope.Operation.UPDATE);
        sb.append(String.join(", ", updateSetValueList));
        sb.append(" where ");
        sb.append(getWhereCondition(tableMetaData, before, Envelope.Operation.DELETE));
        return sb.toString();
    }

    /**
     * Get delete sql
     *
     * @param tableMetaData the table metadata
     * @param before the before struct
     * @return String
     */
    public String getDeleteSql(TableMetaData tableMetaData, Struct before) {
        StringBuilder sb = new StringBuilder();
        sb.append("delete from ").append(tableMetaData.getTableFullName()).append(" where ");
        sb.append(getWhereCondition(tableMetaData, before, Envelope.Operation.DELETE));
        return sb.toString();
    }

    private String getWhereCondition(TableMetaData tableMetaData, Struct before, Envelope.Operation option) {
        List<ColumnMetaData> primaryColumnMetaDataList = tableMetaData.getColumnList()
                .stream()
                .filter(ColumnMetaData::isPrimaryColumn)
                .collect(Collectors.toList());

        ArrayList<String> whereConditionValueList;
        if (primaryColumnMetaDataList.size() > 0) {
            whereConditionValueList = getValueList(primaryColumnMetaDataList, before, option);
        }
        else {
            whereConditionValueList = getValueList(tableMetaData.getColumnList(), before, option);
        }

        StringBuilder sb = new StringBuilder();
        sb.append(String.join(" and ", whereConditionValueList));
        sb.append(";");
        return sb.toString();
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
                    else if ("json".equals(columnType)) {
                        valueList.add(columnName + JSON_PREFIX + "=" + singleValue);
                    }
                    else if ("interval".equals(columnType)) {
                        String intervalType = columnMetaData.getIntervalType();
                        String conditionValue = "interval " + singleValue;
                        if ("DAY TO SECOND".equals(intervalType)) {
                            conditionValue += " second";
                        }
                        else if ("YEAR TO MONTH".equals(intervalType)) {
                            conditionValue += " month";
                        }
                        else {
                            conditionValue = singleValue;
                        }
                        valueList.add(columnName + " = " + conditionValue);
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

    /**
     * Get create table statement
     *
     * @param tableName the table name
     * @param tableChangesField the table changed field
     * @return String
     */
    public String getCreateTableStatement(String tableName, TableChangesField tableChangesField) {
        final SqlStatementBuilder builder = new SqlStatementBuilder();
        builder.append("CREATE TABLE ");
        builder.append(addingQuote(tableName));
        builder.append(" (");

        // columns
        Map<String, TableChangesField.ColumnField> columnMap = tableChangesField.getColumnFieldsMap();
        builder.appendList(columnMap.keySet(), (columnName) -> {
            TableChangesField.ColumnField columnField = columnMap.get(columnName);
            return constructColumnSpec(columnField);
        });

        // primary key
        List<String> primaryKeyColumnNames = tableChangesField.getPrimaryKeyColumnNames();
        if (primaryKeyColumnNames != null && !primaryKeyColumnNames.isEmpty()) {
            builder.append(", PRIMARY KEY(");
            builder.appendList(primaryKeyColumnNames, SqlTools::addingQuote);
            builder.append(")");
        }
        // foreign key
        appendForeignKey(tableChangesField, builder, ",");
        // unique key
        appendUniqueKey(tableChangesField, builder, ",");
        // check
        appendCheck(tableChangesField, builder, ",");
        builder.append(")");
        return builder.build();
    }

    /**
     * Get alter table statement
     *
     * @param tableName the table name
     * @param tableChangesField the table changes field
     * @return String
     */
    public String getAlterTableStatement(String tableName, TableChangesField tableChangesField) {
        final SqlStatementBuilder changeColumnBuilder = new SqlStatementBuilder();
        Map<String, TableChangesField.ColumnField> columnMap = tableChangesField.getColumnFieldsMap();

        // change column
        TableChangesField.ChangeColumnsField changeColumns = tableChangesField.getChangeColumns();
        if (changeColumns != null) {
            List<String> changeColumnNames = changeColumns.getColumnNames();
            String changeType = changeColumns.getChangeType();
            switch (changeType) {
                case TableChangesField.ChangeColumnsField.ADD_COLUMNS:
                    changeColumnBuilder.appendList(changeColumnNames, (name) -> {
                        TableChangesField.ColumnField columnField = columnMap.get(name);
                        return "ADD " + constructColumnSpec(columnField);
                    });
                    break;
                case TableChangesField.ChangeColumnsField.MODIFY_COLUMNS:
                    changeColumnBuilder.appendList(changeColumnNames, (name) -> {
                        TableChangesField.ColumnField columnField = columnMap.get(name);
                        final StringBuilder alterColumnSpec = new StringBuilder();
                        alterColumnSpec.append("ALTER ");
                        alterColumnSpec.append(addingQuote(columnField.getName()));
                        alterColumnSpec.append(" TYPE ");
                        alterColumnSpec.append(constructColumnType(columnField));
                        return alterColumnSpec.toString();
                    });
                    break;
                case TableChangesField.ChangeColumnsField.DROP_COLUMNS:
                    changeColumnBuilder.appendList(changeColumnNames, (name) -> "DROP " + addingQuote(name));
                    break;
            }
        }

        // constraint primary key
        List<TableChangesField.PrimaryKeyColumnChangesField> primaryKeyColumnChanges =
                tableChangesField.getPrimaryKeyColumnChanges();
        if (primaryKeyColumnChanges != null && !primaryKeyColumnChanges.isEmpty()) {
            for (TableChangesField.PrimaryKeyColumnChangesField primaryKeyColumnChange : primaryKeyColumnChanges) {
                String action = primaryKeyColumnChange.getAction();
                String constraintName = primaryKeyColumnChange.getConstraintName();
                String columnName = primaryKeyColumnChange.getColumnName();
                if (TableChangesField.PrimaryKeyColumnChangesField.VALUE_ACTION_ADD.equals(action)) {
                    changeColumnBuilder.append(" ADD ");
                    if (constraintName != null && !constraintName.isEmpty()) {
                        changeColumnBuilder.append("CONSTRAINT ").append(addingQuote(constraintName));
                    }
                    changeColumnBuilder.append(" PRIMARY KEY (").append(addingQuote(columnName)).append(")");
                }
                else if (TableChangesField.PrimaryKeyColumnChangesField.VALUE_ACTION_DROP.equals(action)) {
                    changeColumnBuilder.append(" DROP ");
                    if (constraintName != null && !constraintName.isEmpty()) {
                        changeColumnBuilder.append("CONSTRAINT ").append(addingQuote(constraintName));
                    }
                    else {
                        changeColumnBuilder.append(" PRIMARY KEY");
                    }
                    changeColumnBuilder.append(" ").append(primaryKeyColumnChange.getCascade());
                }
            }
        }
        // constraint foreign key
        appendForeignKey(tableChangesField, changeColumnBuilder, " ADD ");
        // constraint unique key
        appendUniqueKey(tableChangesField, changeColumnBuilder, " ADD ");
        // constraint check
        appendCheck(tableChangesField, changeColumnBuilder, " ADD ");
        // alter default value
        final SqlStatementBuilder defaultValueBuilder = new SqlStatementBuilder();
        defaultValueBuilder.appendList(columnMap.keySet(), (name) -> {
            TableChangesField.ColumnField columnField = columnMap.get(name);
            List<String> modifyKeys = columnField.getModifyKeys();
            if (modifyKeys == null
                    || !modifyKeys.contains(TableChangesField.ColumnField.DEFAULT_VALUE_EXPRESSION)) {
                return "";
            }

            final StringBuilder alterDefaultSpec = new StringBuilder();
            alterDefaultSpec.append("ALTER ");
            alterDefaultSpec.append(addingQuote(name));
            alterDefaultSpec.append(" SET DEFAULT ");
            alterDefaultSpec.append(columnField.getDefaultValueExpression());
            return alterDefaultSpec.toString();
        });

        String changeColumnStatement = changeColumnBuilder.build();
        String defaultValueStatement = defaultValueBuilder.build();
        if (changeColumnStatement.isEmpty() && defaultValueStatement.isEmpty()) {
            // no change
            return "";
        }

        final SqlStatementBuilder sqlBuilder = new SqlStatementBuilder();
        sqlBuilder.append("ALTER TABLE ");
        sqlBuilder.append(addingQuote(tableName));
        sqlBuilder.append(" ");
        sqlBuilder.append(changeColumnStatement);

        if (!defaultValueStatement.isEmpty()) {
            if (!changeColumnStatement.isEmpty()) {
                sqlBuilder.append(",");
            }
            sqlBuilder.append(defaultValueStatement);
        }

        return sqlBuilder.build();
    }

    private static void appendCheck(TableChangesField tableChangesField,
                                    SqlStatementBuilder builder, String concatVal) {
        List<TableChangesField.CheckColumnsField> checkColumns = tableChangesField.getCheckColumns();
        if (checkColumns != null && !checkColumns.isEmpty()) {
            // constraint <indexName> check (<condition>)
            // e.g. condition=":$0 > 18 or :$1 < 10", includeColumn="age,month",
            // :$0 -> age, :$1 -> month
            for (TableChangesField.CheckColumnsField checkColumn : checkColumns) {
                builder.append(concatVal);
                String indexName = checkColumn.getIndexName();
                if (indexName != null && !indexName.isEmpty()) {
                    builder.append("CONSTRAINT ").append(addingQuote(indexName)).append(" ");
                }
                builder.append("check (");
                String condition = checkColumn.getCondition();
                String includeColumn = checkColumn.getIncludeColumn();
                String[] includeColumnSplit = includeColumn.split(",");
                for (int i = 0, size = includeColumnSplit.length; i < size; i++) {
                    condition = condition.replaceAll(":\\$" + i, addingQuote(includeColumnSplit[i]));
                }
                builder.append(condition).append(")");
            }
        }
    }

    private static void appendUniqueKey(TableChangesField tableChangesField,
                                        SqlStatementBuilder builder, String concatVal) {
        List<TableChangesField.UniqueColumnsField> uniqueColumns = tableChangesField.getUniqueColumns();
        if (uniqueColumns != null && !uniqueColumns.isEmpty()) {
            Map<String, List<TableChangesField.UniqueColumnsField>> uniqueColumnMap = uniqueColumns.stream()
                    .collect(Collectors.groupingBy(TableChangesField.UniqueColumnsField::getIndexName));
            uniqueColumnMap.forEach((indexName, uniqueColumnsFields) -> {
                // constraint <indexName> unique (<columnName>)
                List<String> columnNames = uniqueColumnsFields.stream()
                        .map(TableChangesField.UniqueColumnsField::getColumnName)
                        .collect(Collectors.toList());
                builder.append(concatVal).append(" CONSTRAINT ").append(addingQuote(indexName)).append(" unique(");
                builder.appendList(columnNames, SqlTools::addingQuote);
                builder.append(")");
            });
        }
    }

    private void appendForeignKey(TableChangesField tableChangesField,
                                  SqlStatementBuilder builder, String concatValue) {
        List<TableChangesField.ForeignKeyColumnsField> foreignKeyColumns = tableChangesField.getForeignKeyColumns();
        if (foreignKeyColumns != null && !foreignKeyColumns.isEmpty()) {
            for (TableChangesField.ForeignKeyColumnsField field : foreignKeyColumns) {
                // constraint <fkName> foreign key (<fkColumnName>) references
                // <pkTableSchema>.<pkTableName>(<pkColumnName>) <cascade>
                String sinkSchema = schemaMappingMap.getOrDefault(field.getPkTableSchema(), field.getPkTableSchema());
                List<String> fkColumnList = Arrays.asList(field.getFkColumnName().split(","));
                List<String> pkColumnList = Arrays.asList(field.getPkColumnName().split(","));

                builder.append(concatValue).append(" CONSTRAINT ").append(addingQuote(field.getFkName()))
                        .append(" FOREIGN KEY (");
                builder.appendList(fkColumnList, SqlTools::addingQuote);
                builder.append(")").append(" REFERENCES ")
                        .append(addingQuote(sinkSchema))
                        .append(".").append(addingQuote(field.getPkTableName())).append("(");
                builder.appendList(pkColumnList, SqlTools::addingQuote);
                builder.append(") ");
                if (field.getCascade() != null) {
                    builder.append(field.getCascade());
                }
            }
        }
    }

    /**
     * Get drop table statement
     *
     * @param tableName the table name
     * @return String
     */
    public String getDropTableStatement(String tableName) {
        return "DROP TABLE " + addingQuote(tableName);
    }

    private String constructPrecisionAndScale(Integer length, Integer scale) {
        if (length == null) {
            return "";
        }

        StringBuilder builder = new StringBuilder();
        if (scale == null) {
            builder.append("(").append(length).append(")");
        }
        else {
            builder.append("(").append(length).append(",").append(scale).append(")");
        }

        return builder.toString();
    }

    private String constructColumnSpec(TableChangesField.ColumnField columnField) {
        final StringBuilder columnSpec = new StringBuilder();
        columnSpec.append(addingQuote(columnField.getName())).append(" ").append(constructColumnType(columnField));

        String defaultValue = columnField.getDefaultValueExpression();
        if (defaultValue != null) {
            if (defaultValue.equalsIgnoreCase("systimestamp")) {
                defaultValue = "current_timestamp";
            }
            columnSpec.append(" DEFAULT ").append(defaultValue);
        }

        columnSpec.append(columnField.isOptional() ? " NULL" : " NOT NULL");
        return columnSpec.toString();
    }

    private String constructColumnType(TableChangesField.ColumnField columnField) {
        int jdbcType = columnField.getJdbcType();
        String columnType = DebeziumTypeConverters.toDataType(jdbcType);
        if (columnType == null) {
            columnType = columnField.getTypeName();
        }

        String precisionAndScale;
        if ("CHAR".equalsIgnoreCase(columnType) || "NCHAR".equalsIgnoreCase(columnType)
                || "NVARCHAR2".equalsIgnoreCase(columnType) || "VARCHAR2".equalsIgnoreCase(columnType)) {
            precisionAndScale = constructPrecisionAndScale(columnField.getLength(), null);
        } else if (!"FLOAT".equalsIgnoreCase(columnType)
                && !"BLOB".equalsIgnoreCase(columnType)
                && !columnType.toLowerCase(Locale.ROOT).contains("interval")
                && !columnType.toLowerCase(Locale.ROOT).contains("time zone")) {
            precisionAndScale = constructPrecisionAndScale(columnField.getLength(), columnField.getScale());
        } else {
            precisionAndScale = "";
        }

        return columnType + precisionAndScale;
    }
}
