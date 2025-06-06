/*
 * Copyright (c) 2025-2025 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *           http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package org.full.migration.source;

import lombok.EqualsAndHashCode;

import org.apache.commons.lang3.StringUtils;
import org.full.migration.constants.CommonConstants;
import org.full.migration.constants.SqlServerSqlConstants;
import org.full.migration.jdbc.SqlServerConnection;
import org.full.migration.model.TaskTypeEnum;
import org.full.migration.model.config.GlobalConfig;
import org.full.migration.model.table.Column;
import org.full.migration.model.table.GenerateInfo;
import org.full.migration.model.table.PartitionDefinition;
import org.full.migration.model.table.Table;
import org.full.migration.model.table.TableIndex;
import org.full.migration.translator.SqlServerColumnType;
import org.full.migration.translator.SqlServerFuncTranslator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.stream.Collectors;

/**
 * SqlServerSource
 *
 * @since 2025-04-18
 */
@EqualsAndHashCode(callSuper = true)
public class SqlServerSource extends SourceDatabase {
    private static final Logger LOGGER = LoggerFactory.getLogger(SqlServerSource.class);

    public SqlServerSource(GlobalConfig globalConfig) {
        super(globalConfig);
        this.connection = new SqlServerConnection();
    }


    @Override
    public void createSourceLogicalReplicationSlot(Connection conn) {

    }

    @Override
    public void dropSourceLogicalReplicationSlot(Connection conn) {

    }

    /**
     * setReplicaIdentity
     *
     * @param table
     */
    protected void setReplicaIdentity(Table table) {

    }

    @Override
    protected void initPublication(Connection conn, List<String> migraTableNames) {

    }

    @Override
    protected void createCustomOrDomainTypesSql(Connection conn, String schema) {

    }

    @Override
    protected List<Table> getSchemaAllTables(String schema, Connection conn) {
        List<Table> tables = new ArrayList<>();
        String queryTableSql = String.format(SqlServerSqlConstants.QUERY_TABLE_SQL, schema);
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(queryTableSql)) {
            while (rs.next()) {
                String tableName = rs.getString("TableName");
                Table table = new Table(sourceConfig.getDbConn().getDatabase(), schema, tableName);
                table.setTargetSchemaName(sourceConfig.getSchemaMappings().get(schema));
                table.setAveRowLength(rs.getLong("avgRowLength"));
                table.setRowCount(rs.getInt("tableRows"));
                table.setPartition(rs.getBoolean("isPartitioned"));
                tables.add(table);
            }
        } catch (SQLException e) {
            LOGGER.error("fail to query table list, error message:{}.", e.getMessage());
        }
        return tables;
    }

    @Override
    protected Optional<GenerateInfo> getGeneratedDefine(Connection conn, String schema, String tableName,
        String column) {
        try (PreparedStatement pstmt = conn.prepareStatement(SqlServerSqlConstants.QUERY_GENERATE_DEFINE_SQL)) {
            pstmt.setString(1, schema);
            pstmt.setString(2, tableName);
            pstmt.setString(3, column);
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    GenerateInfo generateInfo = new GenerateInfo();
                    generateInfo.setName(column);
                    generateInfo.setIsStored(rs.getBoolean("is_persisted"));
                    generateInfo.setDefine(convertCondition(rs.getString("computation_expression")));
                    return Optional.of(generateInfo);
                }
            }
        } catch (SQLException e) {
            LOGGER.error("query generate define occurred an exception, schema:{}, table:{}, column:{}", schema,
                tableName, column);
        }
        return Optional.empty();
    }

    @Override
    protected String getQueryUniqueConstraint() {
        return SqlServerSqlConstants.QUERY_UNIQUE_CONSTRAINT_SQL;
    }

    @Override
    protected String getQueryCheckConstraint() {
        return SqlServerSqlConstants.QUERY_CHECK_CONSTRAINT_SQL;
    }

    @Override
    public String convertToOpenGaussSyntax(String sqlServerDefinition) {
        return SqlServerFuncTranslator.convertDefinition(sqlServerDefinition);
    }

    @Override
    public boolean isGeometryTypes(String typeName) {
        return SqlServerColumnType.isGeometryTypes(typeName);
    }

    @Override
    public String getColumnDdl(Table table, List<Column> columns) {
        StringJoiner columnDdl = new StringJoiner(", ");
        for (Column column : columns) {
            String colName = column.getName();
            String colType = getColumnType(column);
            if (SqlServerColumnType.isTimesTypes(colType) && !sourceConfig.getIsTimeMigrate()) {
                LOGGER.error("{}.{} has column type {}, don't migrate this table according to the configuration",
                        table.getSchemaName(), table.getTableName(), colType);
                return "";
            }
            String nullType = column.isOptional() ? "" : " NOT NULL ";
            columnDdl.add(String.format("%s %s %s", colName, colType, nullType));
        }
        return columnDdl.toString();
    }

    private String getColumnType(Column column) {
        if (column.isAutoIncremented()) {
            return "serial";
        }
        String typeName = column.getTypeName().split(" ")[0];
        String ogType = SqlServerColumnType.convertType(typeName);
        StringBuilder builder = new StringBuilder(ogType);
        if (SqlServerColumnType.isTypeWithLength(typeName)) {
            long length = column.getLength();
            Integer scale = column.getScale();
            // 大文本类型varchar(max),nvarchar(max),varbinary(max)
            if ((SqlServerColumnType.isVarsTypes(typeName) || SqlServerColumnType.isBinaryTypes(typeName))
                    && length == Integer.MAX_VALUE) {
                return SqlServerColumnType.convertType(typeName + "(max)");
            }
            if (SqlServerColumnType.isTimesTypes(typeName)) {
                if (!sourceConfig.getIsTimeMigrate()) {
                    return typeName;
                }
                builder.append("(").append(scale > 6 ? 6 : scale).append(")");
                if (SqlServerColumnType.SS_DATETIMEOFFSET.getSsType().equals(typeName)) {
                    builder.append(" with time zone ");
                }
            } else {
                // 可变长类型length == Integer.MAX_VALUE时表示没指定长度, numeric类型length==0时没指定长度和精度。
                if (hasLengthLimit(typeName, length)) {
                    builder.append("(").append(length);
                }
                // numeric类型获取scale
                if (SqlServerColumnType.isNumericType(typeName) && length > 0 && scale != null && scale > 0) {
                    builder.append(",").append(scale);
                }
                if (hasLengthLimit(typeName, length)) {
                    builder.append(")");
                }
            }
        }
        if (column.isGenerated()) {
            builder.append("GENERATED ALWAYS AS")
                    .append(column.getGenerateInfo().getDefine())
                    .append(column.getGenerateInfo().getIsStored() ? " STORED " : "VIRTUAL");
        }
        String defaultValue = column.getDefaultValueExpression();
        if (StringUtils.isNoneEmpty(defaultValue)) {
            builder.append(" default ").append(SqlServerFuncTranslator.convertDefinition(defaultValue));
        }
        return builder.toString();
    }

    private static boolean hasLengthLimit(String typeName, long length) {
        return (SqlServerColumnType.isVarsTypes(typeName) && length != Integer.MAX_VALUE) || (
                SqlServerColumnType.isNumericType(typeName) && length > 0) || (!SqlServerColumnType.isVarsTypes(typeName)
                && !SqlServerColumnType.isNumericType(typeName) && !SqlServerColumnType.isBinaryTypes(typeName));
    }

    @Override
    public boolean isPartitionChildTable(String schema, String table, Connection connection){
        return false;
    }

    private static PartitionDefinition getPartitionDefinition(Connection conn, String schema, String table)
        throws SQLException {
        // 获取分区列和分区函数信息
        PartitionDefinition partitionDef = new PartitionDefinition();
        try (PreparedStatement stmt = conn.prepareStatement(SqlServerSqlConstants.QUERY_PARTITION_SQL)) {
            stmt.setString(1, schema);
            stmt.setString(2, table);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    partitionDef.setPartitionColumn(rs.getString("partition_column"));
                    String functionType = rs.getString("function_type");
                    if ("RANGE".equalsIgnoreCase(functionType)) {
                        partitionDef.setPartitionType("RANGE");
                        partitionDef.setRightRange(rs.getBoolean("is_right"));
                    } else if ("LIST".equalsIgnoreCase(functionType)) {
                        partitionDef.setPartitionType("LIST");
                    } else if ("HASH".equalsIgnoreCase(functionType)) {
                        partitionDef.setPartitionType("HASH");
                        partitionDef.setPartitionCount(rs.getInt("partition_count"));
                    } else {
                        partitionDef.setPartitionType("UNKNOWN");
                    }
                }
            }
        }
        // 获取分区边界值(RANGE/LIST)
        if (partitionDef.isRangePartition() || partitionDef.isListPartition()) {
            try (PreparedStatement stmt = conn.prepareStatement(SqlServerSqlConstants.QUERY_PARTITION_BOUNDARY_SQL)) {
                stmt.setString(1, schema);
                stmt.setString(2, table);
                try (ResultSet rs = stmt.executeQuery()) {
                    List<String> boundaries = new ArrayList<>();
                    while (rs.next()) {
                        boundaries.add(rs.getString("boundary_value"));
                    }
                    partitionDef.setBoundaries(boundaries);
                }
            }
        }
        return partitionDef;
    }

    @Override
    public String getParentTables(Connection conn, Table table) throws SQLException {
        return null;
    }

    /**
     * getPartitionDdl
     *
     * @param conn conn,schema schema, tableName tableName
     * @return partitionDdl
     */
    @Override
    public String getPartitionDdl(Connection conn, String schema, String tableName) throws SQLException {
        PartitionDefinition partitionDef = getPartitionDefinition(conn, schema, tableName);
        StringBuilder partitionDdl = new StringBuilder("\n PARTITION BY ").append(partitionDef.getPartitionType())
                .append(" (")
                .append(partitionDef.getPartitionColumn())
                .append(")");
        if (partitionDef.isRangePartition()) {
            partitionDdl.append(generateRangePartitionDdl(partitionDef));
        } else if (partitionDef.isListPartition()) {
            partitionDdl.append(generateListPartitionDdl(partitionDef));
        } else if (partitionDef.isHashPartition()) {
            partitionDdl.append(generateHashPartitionDdl(partitionDef));
        } else {
            return null;
        }
        return partitionDdl.toString();
    }

    private String generateRangePartitionDdl(PartitionDefinition partitionDef) {
        StringBuilder rangePartitionDdl = new StringBuilder();
        rangePartitionDdl.append(" (");
        List<String> boundaries = partitionDef.getBoundaries();
        for (int i = 0; i <= boundaries.size(); i++) {
            String partitionName = "p" + i;
            String condition;
            if (i == boundaries.size()) {
                condition = "VALUES LESS THAN (MAXVALUE)";
            } else {
                condition = "VALUES LESS THAN ('" + boundaries.get(i) + "')";
            }
            rangePartitionDdl.append("\n  PARTITION ").append(partitionName).append(" ").append(condition).append(",");
        }
        rangePartitionDdl.deleteCharAt(rangePartitionDdl.length() - 1);
        rangePartitionDdl.append("\n)");
        return rangePartitionDdl.toString();
    }

    private String generateListPartitionDdl(PartitionDefinition partitionDef) {
        StringBuilder listPartitionDdl = new StringBuilder();
        listPartitionDdl.append(" (");
        List<String> boundaries = partitionDef.getBoundaries();
        for (int i = 0; i < boundaries.size(); i++) {
            String partitionName = "p" + i;
            listPartitionDdl.append("\n  PARTITION ")
                    .append(partitionName)
                    .append(" VALUES (")
                    .append(boundaries.get(i))
                    .append("),");
        }
        // 添加默认分区
        listPartitionDdl.append("\n  PARTITION p_default VALUES (DEFAULT)");
        listPartitionDdl.append("\n)");
        return listPartitionDdl.toString();
    }

    private String generateHashPartitionDdl(PartitionDefinition partitionDef) {
        StringBuilder hashPartitionDdl = new StringBuilder();
        hashPartitionDdl.append(" (");
        int partitionCount = partitionDef.getPartitionCount();
        for (int i = 0; i < partitionCount; i++) {
            hashPartitionDdl.append("\n  PARTITION p").append(i).append(",");
        }
        hashPartitionDdl.deleteCharAt(hashPartitionDdl.length() - 1);
        hashPartitionDdl.append("\n)");
        return hashPartitionDdl.toString();
    }

    @Override
    protected String getIsolationSql() {
        return SqlServerSqlConstants.SET_SNAPSHOT_SQL;
    }

    @Override
    protected void lockTable(Table table, Connection conn) throws SQLException {

    }

    @Override
    protected void readAndSendXlogLocation(Connection conn, Table table) throws SQLException, InterruptedException {

    }

    @Override
    protected String getQueryWithLock(Table table, List<Column> columns, Connection conn) {
        List<String> columnNames = columns.stream().map(column -> {
            String name = column.getName();
            if (SqlServerColumnType.isGeometryTypes(column.getTypeName())) {
                return name + ".STAsText() AS " + name;
            }
            return name;
        }).collect(Collectors.toList());
        return String.format(SqlServerSqlConstants.QUERY_WITH_LOCK_SQL,
            String.join(CommonConstants.DELIMITER, columnNames), table.getCatalogName(), table.getSchemaName(),
            table.getTableName());
    }

    @Override
    protected String getSnapShotPoint(Connection conn) {
        return SqlServerSqlConstants.MAX_LSN_SQL;
    }

    @Override
    protected String getQueryObjectSql(String objectType) throws IllegalArgumentException {
        switch (objectType) {
            case "view":
                return SqlServerSqlConstants.QUERY_VIEW_SQL;
            case "function":
                return SqlServerSqlConstants.QUERY_FUNCTION_SQL;
            case "trigger":
                return SqlServerSqlConstants.QUERY_TRIGGER_SQL;
            case "procedure":
                return SqlServerSqlConstants.QUERY_PROCEDURE_SQL;
            case "sequence":
                return SqlServerSqlConstants.QUERY_SEQUENCE_SQL;
            default:
                LOGGER.error(
                    "objectType {} is invalid, please check the object of migration in [view, function, trigger, "
                        + "procedure, sequence]", objectType);
                throw new IllegalArgumentException(objectType + "is an unsupported type.");
        }
    }

    @Override
    protected String convertDefinition(String objectType, ResultSet rs) throws SQLException {
        if (TaskTypeEnum.SEQUENCE.getTaskType().equalsIgnoreCase(objectType)) {
            long minValue = rs.getLong("minValue");
            long maxValue = rs.getLong("maxValue");
            int typeId = rs.getInt("typeId");
            if (minValue == Long.MIN_VALUE || maxValue == Long.MAX_VALUE) {
                long[] bounds = getDataTypeBounds(typeId);
                minValue = Math.max(bounds[0], minValue);
                maxValue = Math.min(bounds[1], maxValue);
            }
            int cacheSize = rs.getInt("cacheSize");
            // SQLServer 默认50
            cacheSize = cacheSize == 0 ? 50 : cacheSize;
            long increment = rs.getLong("increment");
            increment = increment == 0 ? 1 : increment;
            long startValue = rs.getLong("startValue");
            startValue = startValue == 0 ? (increment > 0 ? 1 : -1) : startValue;
            boolean isCycling = rs.getBoolean("isCycling");
            long currentValue = rs.getLong("currentValue");
            return String.format(Locale.ROOT,
                "CREATE SEQUENCE IF NOT EXISTS %s START WITH %d INCREMENT BY %d MINVALUE %d MAXVALUE %d %s CACHE %d; "
                    + "SELECT setval('%s', %d);", rs.getString("name"), startValue, increment, minValue, maxValue,
                isCycling ? "CYCLE" : "NOCYCLE", cacheSize, rs.getString("name"), currentValue);
        }
        return rs.getString("definition");
    }

    private long[] getDataTypeBounds(int systemTypeId) {
        switch (systemTypeId) {
            case 56: // int
                return new long[] {-2147483648L, 2147483647L};
            case 52: // smallint
                return new long[] {-32768L, 32767L};
            case 48: // tinyint
                return new long[] {0L, 255L};
            case 127: // bigint
                return new long[] {-9223372036854775808L, 9223372036854775807L};
            default:
                throw new IllegalArgumentException("Unsupported data type ID: " + systemTypeId);
        }
    }

    @Override
    protected String getQueryIndexSql(String schema) {
        return String.format(SqlServerSqlConstants.QUERY_INDEX_SQL, schema);
    }

    @Override
    protected TableIndex getTableIndex(Connection conn, ResultSet rs) throws SQLException {
        TableIndex tableIndex = new TableIndex(rs);
        if (tableIndex.isHasFilter() && StringUtils.isNotEmpty(tableIndex.getFilterDefinition())) {
            tableIndex.setFilterDefinition(convertCondition(tableIndex.getFilterDefinition()));
        }
        long objectId = rs.getLong("object_id");
        List<String> indexCols = new ArrayList<>();
        try (Statement stmt = conn.createStatement();
            ResultSet colRs = stmt.executeQuery(
                String.format(SqlServerSqlConstants.QUERY_INDEX_COL_SQL, objectId, objectId,
                    tableIndex.getIndexName()))) {
            while (colRs.next()) {
                indexCols.add(colRs.getString("name"));
            }
            tableIndex.setColumnName(String.join(CommonConstants.DELIMITER, indexCols));
        }
        return tableIndex;
    }

    @Override
    protected String getQueryPkSql() {
        return SqlServerSqlConstants.QUERY_PRIMARY_KEY_SQL;
    }

    @Override
    protected String getQueryFkSql(String schema) {
        return String.format(SqlServerSqlConstants.QUERY_FOREIGN_KEY_SQL, schema);
    }

    private String convertCondition(String definition) {
        return definition.replace("[", "").replace("]", "");
    }
}
