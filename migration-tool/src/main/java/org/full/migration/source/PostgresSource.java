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
import org.full.migration.constants.CommonConstants;
import org.full.migration.constants.PostgresSqlConstants;
import org.full.migration.jdbc.PostgresConnection;
import org.full.migration.model.PostgresCustomTypeMeta;
import org.full.migration.model.TaskTypeEnum;
import org.full.migration.model.config.GlobalConfig;
import org.full.migration.model.table.*;
import org.full.migration.model.table.postgres.partition.HashPartitionInfo;
import org.full.migration.model.table.postgres.partition.ListPartitionInfo;
import org.full.migration.model.table.postgres.partition.PartitionInfo;
import org.full.migration.model.table.postgres.partition.RangePartitionInfo;
import org.full.migration.translator.PostgresColumnType;
import org.full.migration.translator.PostgresqlFuncTranslator;
import org.full.migration.utils.DatabaseUtils;
import org.postgresql.core.ServerVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.StringJoiner;
import java.util.Optional;
import java.util.Set;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * PostgresSource
 *
 * @author zhaochen
 * @since 2025-05-12
 */
@EqualsAndHashCode(callSuper = true)
public class PostgresSource extends SourceDatabase {
    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresSource.class);
    private static final String LINESEP = System.lineSeparator();
    private static final Map<String, String> typeConvertMap = new HashMap<String, String>() {
        {
            put("line", "varchar");
            put("pg_lsn", "varchar");
            put("macaddr8", "varchar");
            put("regrole", "varchar");
            put("regnamespace", "varchar");
        }
    };
    public PostgresSource(GlobalConfig globalConfig) {
        super(globalConfig);
        this.connection = new PostgresConnection();
    }

    /**
     * initPublication
     *
     * @param conn
     * @param migraTableNames
     */
    @Override
    protected void initPublication(Connection conn, List<String> migraTableNames) {
        String createPublicationStmt;
        String migraTableString = null;
        try {
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(PostgresSqlConstants.SELECT_PUBLICATION)) {
                if (rs.next()) {
                    Long count = rs.getLong(1);
                    if (count != 0L) {
                        LOGGER.warn(
                                "A logical publication named dbz_publication for plugin '{}' and database '{}' is " +
                                        "already active on the server, and drop the old publication 'dbz_publication'",
                                sourceConfig.getPluginName(), sourceConfig.getDbConn().getDatabase());
                        stmt.execute(PostgresSqlConstants.DROP_PUBLICATION);
                    }
                    LOGGER.info("Creating new publication dbz_publication for plugin '{}'", sourceConfig.getPluginName());
                    try {
                        migraTableString = String.join(", ", migraTableNames);;
                        if (migraTableString.isEmpty()) {
                            LOGGER.warn("No table found for publication dbz_publication");
                        }
                        createPublicationStmt = String.format(PostgresSqlConstants.CREATE_PUBLICATION, migraTableString);
                        LOGGER.info("Creating Publication with statement '{}'", createPublicationStmt);
                        // Publication doesn't exist, create it but restrict to the tableFilter.
                        stmt.execute(createPublicationStmt);
                    } catch (Exception e) {
                        LOGGER.error("Unable to create filtered publication dbz_publication for {}. error message:{}",
                                migraTableString, e.getMessage());
                    }
                }
            }
        } catch (SQLException e) {
            LOGGER.error("Failed to check if Publication 'dbz_publication' already exists. error message:{}", e.getMessage());
        }
    }

    /**
     * createSourceLogicalReplicationSlot
     *
     * @param conn
     */
    @Override
    public void createSourceLogicalReplicationSlot(Connection conn) {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(String.format(PostgresSqlConstants.PG_CREATE_LOGICAL_REPLICATION_SLOT,
                     sourceConfig.getSlotName(), sourceConfig.getPluginName()))) {
            LOGGER.info("create logical replication slot {} success.", sourceConfig.getSlotName());
        } catch (SQLException e) {
            LOGGER.error("fail to create logical replication slot, error message:{}.", e.getMessage());
        }
    }

    /**
     * hasSourceLogicalReplicationSlot
     *
     * @param conn
     * @return boolean
     */
    private boolean hasSourceLogicalReplicationSlot(Connection conn) {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(String.format(
                     PostgresSqlConstants.PG_GET_LOGICAL_REPLICATION_SLOT, sourceConfig.getSlotName()))) {
            if (!rs.wasNull()) {
                return true;
            }
        } catch (SQLException e) {
            LOGGER.warn("fail to get logical replication slot, error message:{}.", e.getMessage());
        }
        return false;
    }

    /**
     * dropSourceLogicalReplicationSlot
     *
     * @param conn
     */
    @Override
    public void dropSourceLogicalReplicationSlot(Connection conn) {
        try (Statement stmt = conn.createStatement()) {
            if (hasSourceLogicalReplicationSlot(conn)) {
                stmt.executeQuery(String.format(PostgresSqlConstants.PG_DROP_LOGICAL_REPLICATION_SLOT,
                        sourceConfig.getSlotName()));
            }
        } catch (SQLException e) {
            LOGGER.warn("fail to drop logical replication slot, error message:{}.", e.getMessage());
        }
    }

    /**
     * setReplicaIdentity
     *
     * @param table
     */
    @Override
    protected void setReplicaIdentity(Table table) {
        String enableReplicaStatement = PostgresSqlConstants.PG_SET_TABLE_REPLICA_IDNTITY_FULL;
        try (Connection conn = connection.getConnection(sourceConfig.getDbConn())) {
            if (!table.isHasPrimaryKey()) {
                String sql = String.format(enableReplicaStatement, table.getSchemaName() + "." + table.getTableName());
                LOGGER.warn("Table '{}' does not has a primary key, will enable replica identity full",
                        table.getSchemaName() + "." + table.getTableName());
                conn.createStatement().execute(sql);
            }
        } catch (SQLException e) {
            LOGGER.error("fail to replica identity full. error message:{}.", e.getMessage());
        }

    }

    /**
     * getSchemaAllTables
     *
     * @param schema schema
     * @param conn conn
     * @return tables
     */
    @Override
    protected List<Table> getSchemaAllTables(String schema, Connection conn) {
        List<Table> tables = new ArrayList<>();
        String queryTableSql = String.format(PostgresSqlConstants.QUERY_TABLE_SQL, schema);
        try (Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(queryTableSql)) {
            while (rs.next()) {
                String tableName = rs.getString("TableName");
                Table table = new Table(sourceConfig.getDbConn().getDatabase(), schema, tableName);
                table.setTargetSchemaName(sourceConfig.getSchemaMappings().get(schema));
                if (rs.getBoolean("isPartitioned")) {
                    Map<String, Long> partitionTableSize = getPartitionTableSize(schema, tableName, conn);
                    table.setAveRowLength(partitionTableSize.get("avgRowLength"));
                    table.setRowCount(partitionTableSize.get("tableRows"));
                } else {
                    table.setAveRowLength(rs.getLong("avgRowLength"));
                    table.setRowCount(rs.getInt("tableRows"));
                }
                table.setPartition(rs.getBoolean("isPartitioned"));
                table.setSubPartition(false);
                table.setHasPrimaryKey(rs.getBoolean("hasPrimaryKey"));
                tables.add(table);
            }
        } catch (SQLException e) {
            LOGGER.error("fail to query table list, error message:{}.", e.getMessage());
        }
        return tables;
    }

    /**
     * createCustomOrDomainTypesSql
     *
     * @param conn conn
     * @param schema schema
     * @return postgresCustomTypeMetas
     */
    @Override
    protected List<PostgresCustomTypeMeta> createCustomOrDomainTypesSql(Connection conn, String schema) {
        List<PostgresCustomTypeMeta> postgresCustomTypeMetas = new ArrayList<>();
        postgresCustomTypeMetas.addAll(createCompositeTypeSql(conn, schema));
        postgresCustomTypeMetas.addAll(createEnumTypeSql(conn, schema));
        postgresCustomTypeMetas.addAll(createDomainTypeSql(conn, schema));
        LOGGER.info("end to read custom or domain types.");
        return postgresCustomTypeMetas;
    }

    /**
     * createCompositeTypeSql
     *
     * @param conn conn
     * @param schema schema
     * @return postgresCustomTypeMetas
     */
    private List<PostgresCustomTypeMeta> createCompositeTypeSql(Connection conn, String schema) {
        List<PostgresCustomTypeMeta> postgresCustomTypeMetas = new ArrayList<>();
        String compositeTypeQuery = PostgresSqlConstants.QUERY_COMPOSITE_TYPE_SQL;
        try (PreparedStatement stmt = conn.prepareStatement(compositeTypeQuery)) {
            stmt.setString(1, schema);
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    String compositeTypeName = rs.getString("type_name");
                    String typeDefinition = rs.getString("type_definition");
                    String typeComment = rs.getString("type_comment");

                    if (typeComment != null && !typeComment.isEmpty()) {
                        typeDefinition += "\nCOMMENT ON TYPE " + schema + "." + compositeTypeName +
                                " IS '" + typeComment.replace("'", "''") + "';";
                    }

                    postgresCustomTypeMetas.add(new PostgresCustomTypeMeta(schema, compositeTypeName, typeDefinition));
                }
                return postgresCustomTypeMetas;
            }
        } catch (SQLException e) {
            LOGGER.error("Failed to query composite types for schema {}: {}", schema, e.getMessage());
        }
        return postgresCustomTypeMetas;
    }

    /**
     * createEnumTypeSql
     *
     * @param conn conn
     * @param schema schema
     * @return postgresCustomTypeMetas
     */
    private List<PostgresCustomTypeMeta> createEnumTypeSql(Connection conn, String schema) {
        List<PostgresCustomTypeMeta> postgresCustomTypeMetas = new ArrayList<>();
        String enumTypeQuery = PostgresSqlConstants.QUERY_ENUM_TYPE_SQL;
        try (PreparedStatement stmt = conn.prepareStatement(enumTypeQuery)) {
            stmt.setString(1, schema);
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    String enumTypeName = rs.getString("enum_name");
                    String enumValues = formatEnumValues(rs.getString("enum_values"));
                    String enumComment = rs.getString("enum_comment");

                    StringBuilder sqlBuilder = new StringBuilder();
                    sqlBuilder.append("CREATE TYPE ").append(schema).append(".")
                            .append(enumTypeName).append(" AS ENUM (")
                            .append(enumValues).append(");");

                    if (enumComment != null && !enumComment.isEmpty()) {
                        sqlBuilder.append("\nCOMMENT ON TYPE ").append(schema).append(".")
                                .append(enumTypeName).append(" IS '")
                                .append(enumComment.replace("'", "''")).append("';");
                    }

                    postgresCustomTypeMetas.add(new PostgresCustomTypeMeta(schema, enumTypeName, sqlBuilder.toString()));
                }
                return postgresCustomTypeMetas;
            }
        } catch (SQLException e) {
            LOGGER.error("Failed to query enum types for schema {}: {}", schema, e.getMessage());
        }
        return postgresCustomTypeMetas;
    }

    /**
     * createDomainTypeSql
     *
     * @param conn conn
     * @param schema schema
     * @return postgresCustomTypeMetas
     */
    private List<PostgresCustomTypeMeta> createDomainTypeSql(Connection conn, String schema) {
        List<PostgresCustomTypeMeta> postgresCustomTypeMetas = new ArrayList<>();
        String domainTypeQuery = PostgresSqlConstants.QUERY_DOMAIN_TYPE_SQL;
        try (PreparedStatement stmt = conn.prepareStatement(domainTypeQuery)) {
            stmt.setString(1, schema);
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    String domainTypeName = rs.getString("domain_name");
                    String baseType = rs.getString("base_type");
                    boolean notNull = rs.getBoolean("not_null");
                    String checkConstraint = rs.getString("check_constraint");
                    String domainComment = rs.getString("domain_comment");

                    StringBuilder sqlBuilder = new StringBuilder();
                    sqlBuilder.append("CREATE DOMAIN ").append(schema).append(".").append(domainTypeName)
                            .append(" AS ").append(baseType);

                    if (notNull) sqlBuilder.append(" NOT NULL");
                    if (checkConstraint != null && !checkConstraint.isEmpty()) {
                        sqlBuilder.append(" " + checkConstraint);
                    }
                    sqlBuilder.append(";");

                    if (domainComment != null && !domainComment.isEmpty()) {
                        sqlBuilder.append("\nCOMMENT ON DOMAIN ").append(schema).append(".").append(domainTypeName)
                                .append(" IS '").append(domainComment.replace("'", "''")).append("';");
                    }

                    postgresCustomTypeMetas.add(new PostgresCustomTypeMeta(schema, domainTypeName, sqlBuilder.toString()));
                }
                return postgresCustomTypeMetas;
            }
        } catch (SQLException e) {
            LOGGER.error("Failed to query domain types for schema {}: {}", schema, e.getMessage());
        }
        return postgresCustomTypeMetas;
    }

    /**
     * formatEnumValues
     *
     * @param arrayAggResult arrayAggResult
     * @return enumvalues
     */
    private String formatEnumValues(String arrayAggResult) {
        // 移除大括号
        String withoutBraces = arrayAggResult.replaceAll("[{}]", "");
        // 分割各个枚举值
        String[] values = withoutBraces.split(",");

        StringBuilder sb = new StringBuilder();
        for (String value : values) {
            if (sb.length() > 0) sb.append(", ");

            // 已经带引号的值保持原样，不带引号的加单引号
            if (value.startsWith("\"") && value.endsWith("\"")) {
                sb.append("'").append(value.substring(1, value.length()-1)).append("'");
            } else {
                sb.append("'").append(value).append("'");
            }
        }
        return sb.toString();
    }

    /**
     * getPartitionTableSize
     *
     * @param schemaName schemaName
     * @param tableName tableName
     * @param connection connection
     * @return partitionTableSize
     */
    private Map<String, Long> getPartitionTableSize(String schemaName, String tableName, Connection connection) {
        Map<String, Long> result = new HashMap<>();
            result.put("avgRowLength", 0L);
            result.put("tableRows", 0L);
        try (Statement stmt = connection.createStatement();
             ResultSet rst = stmt.executeQuery(
                     String.format(PostgresSqlConstants.QUERY_PATITION_TABLE_SIZE_SQL, schemaName, tableName))) {
            if (rst.next()) {
                result.put("avgRowLength", rst.getLong("avgRowLength"));
                result.put("tableRows", rst.getLong("tableRows"));
            }
        } catch (SQLException e) {
            LOGGER.error("Error fetching partition table size for {}.{}", schemaName, tableName, e);
        }
        return result;
    }

    @Override
    protected boolean IsColumnGenerate(Connection conn, String schema, String tableName, Column column) {
        return column.isGenerated();
    }

    /**
     * getGeneratedDefine
     *
     * @param conn conn
     * @param schema schema
     * @param tableName tableName
     * @param column column
     * @return generatedDefine
     */
    @Override
    protected Optional<GenerateInfo> getGeneratedDefine(Connection conn, String schema, String tableName, String column) {
        try (PreparedStatement pstmt = conn.prepareStatement(PostgresSqlConstants.QUERY_GENERATE_DEFINE_SQL)) {
            pstmt.setString(1, schema);
            pstmt.setString(2, tableName);
            pstmt.setString(3, column);
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    GenerateInfo generateInfo = new GenerateInfo();
                    generateInfo.setName(column);
                    generateInfo.setIsStored(rs.getBoolean("is_stored"));  // PostgreSQL uses 'is_stored'
                    generateInfo.setDefine(rs.getString("computation_expression"));  // Same logic for conversion
                    return Optional.of(generateInfo);
                }
            }
        } catch (SQLException e) {
            LOGGER.error("query generate define occurred an exception, schema:{}, table:{}, column:{}", schema, tableName, column, e);
        }
        return Optional.empty();
    }

    /**
     * getQueryUniqueConstraint
     *
     * @return uniqueConstraintSql
     */
    @Override
    protected String getQueryUniqueConstraint() {
        return PostgresSqlConstants.QUERY_UNIQUE_CONSTRAINT_SQL;
    }

    /**
     * getQueryCheckConstraint
     *
     * @return checkConstraintSql
     */
    @Override
    protected String getQueryCheckConstraint() {
        return PostgresSqlConstants.QUERY_CHECK_CONSTRAINT_SQL;
    }

    /**
     * convertToOpenGaussSyntax
     *
     * @param PostgresSqlDefinition PostgresSqlDefinition
     * @return PostgresSqlDefinition
     */
    @Override
    public String convertToOpenGaussSyntax(String PostgresSqlDefinition) {
        return PostgresqlFuncTranslator.convertDefinition(PostgresSqlDefinition);
    }

    /**
     * isGeometryTypes
     *
     * @param typeName typeName
     * @return isGeometryTypes
     */
    @Override
    public boolean isGeometryTypes(String typeName) {
        return PostgresColumnType.isGeometryTypes(typeName);
    }

    /**
     * getColumnDdl
     *
     * @param table table
     * @param columns columns
     * @return columnDdl
     */
    @Override
    public String getColumnDdl(Table table, List<Column> columns) {
        StringJoiner columnDdl = new StringJoiner(", ");
        for (Column column : columns) {
            String colName = column.getName();
            //custom type is "schema"."typeName"
            String colType = getColumnType(column).replace("\"" + table.getSchemaName() + "\"", "\"" + table.getTargetSchemaName() + "\"");
            if (PostgresColumnType.isTimesTypes(colType) && !sourceConfig.getIsTimeMigrate()) {
                LOGGER.error("{}.{} has column type {}, don't migrate this table according to the configuration",
                        table.getSchemaName(), table.getTableName(), colType);
                return "";
            }
            String nullType = column.isOptional() ? "" : " NOT NULL ";
            columnDdl.add(String.format("\"%s\" %s %s", colName, colType, nullType));
        }
        return columnDdl.toString();
    }

    /**
     * getColumnType
     *
     * @param column column
     * @return columnStr
     */
    private String getColumnType(Column column) {
        String typeName = convertType(column.getTypeName().split(" ")[0]);
        StringBuilder builder = new StringBuilder(typeName);
        // 判断是否有精度
        if (PostgresColumnType.isTypeWithLength(typeName)) {
            // 获取字段长度
            long length = column.getLength();
            Integer scale = column.getScale();
            // time相关类型获取的是scale而不是length
            if (PostgresColumnType.isTimesTypes(typeName)) {
                builder.append("(").append(scale).append(")");
            } else if (PostgresColumnType.isTypesInterval(typeName)) {
                if (column.getIntervalType() != null) {
                    builder.append(" " + column.getIntervalType());
                } else {
                    builder.append("(").append(scale).append(")");
                }
            } else {
                // 可变长类型length == Integer.MAX_VALUE时表示没指定长度, numberic类型length==0时没指定长度和精度。
                if ((PostgresColumnType.isVarsTypes(typeName) && length != Integer.MAX_VALUE)
                        || (PostgresColumnType.isNumericType(typeName) && length > 0)
                        || (!PostgresColumnType.isVarsTypes(typeName) && !PostgresColumnType.isNumericType(typeName))) {
                    builder.append("(").append(length);
                }
                // numeric类型获取scale
                if (PostgresColumnType.isNumericType(typeName) && length > 0 && scale != null && scale > 0) {
                    builder.append(",").append(scale);
                }
                if ((PostgresColumnType.isVarsTypes(typeName) && length != Integer.MAX_VALUE)
                        || (PostgresColumnType.isNumericType(typeName) && length != 0)
                        || (!PostgresColumnType.isVarsTypes(typeName) && !PostgresColumnType.isNumericType(typeName))) {
                    builder.append(")");
                }
            }
        }

        if (column.isGenerated()) {
            builder.append("GENERATED ALWAYS AS (")
                    .append(column.getGenerateInfo().getDefine())
                    .append(")")
                    .append(column.getGenerateInfo().getIsStored() ? " STORED " : "VIRTUAL");
        } else if (!PostgresColumnType.isSerialTypes(typeName)
                && column.getDefaultValueExpression() != null) {
            builder.append(" default " + column.getDefaultValueExpression());
        }
        return builder.toString();
    }

    /**
     * convertType
     *
     * @param typeName typeName
     * @return typeName
     */
    private String convertType(String typeName) {
        if (typeConvertMap.get(typeName) != null) {
            return typeConvertMap.get(typeName);
        }
        return typeName;
    }

    /**
     * getServerVersionNum
     *
     * @param versionStr versionStr
     * @return version
     */
    private int getServerVersionNum(String versionStr) {
        return ServerVersion.from(versionStr).getVersionNum();
    }

    /**
     * getCurrentServerVersion
     *
     * @param conn conn
     * @return version
     */
    protected int getCurrentServerVersion(Connection conn) {
        String pgServerVersion = "";
        try (Statement stmt = conn.createStatement();
             ResultSet rst = stmt.executeQuery(PostgresSqlConstants.PG_SHOW_SERVER_VERSION)) {
            if (rst.next()) {
                pgServerVersion = rst.getString(1);
            }
        }catch (SQLException e) {
            LOGGER.error("get current server version failed. errMsg: {}", e.getMessage());
        }
        return getServerVersionNum(pgServerVersion);
    }

    /**
     * isPartitionChildTable
     *
     * @param schema schema
     * @param table table
     * @param connection connection
     * @return hasPartition
     */
    @Override
    public boolean isPartitionChildTable(String schema, String table, Connection connection){
        // below pg10 no exist partition table
        boolean hasPartition = false;
        try (Statement stmt = connection.createStatement();
             ResultSet rst = stmt.executeQuery(String.format(PostgresSqlConstants.HAVE_PARTITION_SQL, schema, table))) {
            if (getCurrentServerVersion(connection) < getServerVersionNum(PostgresSqlConstants.PG_SERVER_V10)) {
                LOGGER.debug("The current version {} does not support partitioned tables.", getCurrentServerVersion(connection));
                return false;
            }
            if (rst.next()) {
                hasPartition = rst.getBoolean(1);
            } else {
                LOGGER.error("cannot get partition info of table {}.{}", schema, table);
            }
            return hasPartition;
        } catch (SQLException e) {
            LOGGER.error("get partition info of table {}.{} occurred SQLException. errMsg: {}", schema, table, e.getMessage());
        }
        return hasPartition;
    }

    /**
     * getParentTables
     *
     * @param connection connection
     * @param table table
     * @return parents
     */
    @Override
    public String getParentTables(Connection connection, Table table) {
        String schemaName = table.getSchemaName();
        String tableName = table.getTableName();
        StringJoiner parents = new StringJoiner(",");
        try (Statement stmt = connection.createStatement();
             ResultSet rst = stmt.executeQuery(
                     String.format(PostgresSqlConstants.GET_PARENT_TABLE, schemaName, tableName))) {
            while (rst.next()) {
                parents.add(rst.getString(1));
            }
        } catch (SQLException e) {
            LOGGER.error("get parent tables for table {}.{} failed, errMsg: {}", schemaName, tableName, e.getMessage());
        }
        return parents.toString();
    }

    /**
     * getPartitionDdl
     *
     * @param conn conn,schema schema, tableName tableName
     * @return partitionDdl
     */
    @Override
    public String getPartitionDdl(Connection conn, String schemaName, String tableName, boolean isSubPartition) throws SQLException{
        if (getCurrentServerVersion(conn)
                < ServerVersion.from(PostgresSqlConstants.PG_SERVER_V10).getVersionNum()) {
            return "";
        }
        String partitionKey = getPartitionKey(schemaName, tableName, conn);
        if ("".equals(partitionKey)) {
            return "";
        }

        List<String> childs = getChildTables(schemaName, tableName, conn);
        List<PartitionInfo> firstPartitons = getPartitionInfos(childs, partitionKey, schemaName, tableName, conn);
        String partitionDdl = "partition by " + partitionKey + " ";
        if (partitionKey.startsWith(PartitionInfo.RANGE_PARTITION)) {
            partitionDdl += getRangePartitionDdl(childs, firstPartitons);
        } else if (partitionKey.startsWith(PartitionInfo.LIST_PARTITION)) {
            partitionDdl += getListPartitionDdl(firstPartitons);
        } else if (partitionKey.startsWith(PartitionInfo.HASH_PARTITION)) {
            partitionDdl += getHashPartitionDdl(firstPartitons);
        } else {
            throw new IllegalStateException("Unknown partition type: " + partitionKey);
        }
        return partitionDdl;
    }

    private String getRangePartitionDdl(List<String> childs, List<PartitionInfo> partitions) {
        Map<String, List<String>> partitionToBoundsMap = new HashMap<>();
        AtomicInteger childTableIndex = new AtomicInteger();
        partitions.forEach(rangePartitionInfo -> {
            String lowerBound = rangePartitionInfo.getRangeLowerBound();
            String upperBound = rangePartitionInfo.getRangeUpperBound();

            // 将下限和上限作为列表添加到分区名称的映射中
            partitionToBoundsMap
                    .computeIfAbsent(childs.get(childTableIndex.get()), k -> new ArrayList<>())
                    .add(lowerBound);
            partitionToBoundsMap
                    .computeIfAbsent(childs.get(childTableIndex.get()), k -> new ArrayList<>())
                    .add(upperBound);
            childTableIndex.addAndGet(1);
        });

        List<String> partitionNames = new ArrayList<>(partitionToBoundsMap.keySet());
        Collections.sort(partitionNames);

        StringBuilder builder = new StringBuilder("( ");

        for (String partitionName : partitionNames) {
            List<String> bounds = partitionToBoundsMap.get(partitionName);
            String upperBound = bounds.get(1);

            builder.append(String.format("PARTITION %s VALUES LESS THAN %s," + LINESEP, partitionName, upperBound));
        }
        builder.deleteCharAt(builder.lastIndexOf(","));
        builder.append(")");
        return builder.toString();
    }


    private String getListPartitionDdl(List<PartitionInfo> partitions) {
        if (partitions.isEmpty()) {
            return "";
        }
        String partitionStr = "( ";
        for (PartitionInfo listPartitionInfo : partitions) {
            partitionStr += String.format("partition %s values %s," + LINESEP,
                    listPartitionInfo.getPartitionTable(), listPartitionInfo.getListPartitionValue());
        }
        partitionStr = partitionStr.substring(0, partitionStr.lastIndexOf(","));
        partitionStr += ")";
        return partitionStr;
    }

    private String getHashPartitionDdl(List<PartitionInfo> partitions) {
        if (partitions.isEmpty()) {
            return "";
        }
        Integer partitionIdx = 1;
        String partitionStr = "( ";
        for (int i = 0; i < partitions.size(); ++i) {
            partitionStr += String.format("partition p%s," + LINESEP, partitionIdx);
            partitionIdx += 1;
        }
        partitionStr = partitionStr.substring(0, partitionStr.lastIndexOf(","));
        partitionStr += ")";
        return partitionStr;
    }

    private Set<String> getSubPartitionKeySet(List<PartitionInfo> firstPartitons) {
        Set<String> subPartitionKeySet = new HashSet<>();
        for (PartitionInfo childPartition : firstPartitons) {
            String partitionKey = childPartition.getPartitionKey();
            if (partitionKey == null) {
                continue;
            }
            subPartitionKeySet.add(partitionKey);
        }
        // size=0 means no subPartition
        return subPartitionKeySet;
    }

    private List<PartitionInfo> getPartitionInfos(List<String> partitionTables, String partitionKey, String schema,
                                                  String parentTable, Connection connection) {
        List<PartitionInfo> partitionInfoList = new ArrayList<>();

        for (String table : partitionTables) {
            PartitionInfo partitionInfo;
            if (partitionKey.startsWith(PartitionInfo.RANGE_PARTITION)) {
                partitionInfo = new RangePartitionInfo();
                getRangePartitionValues(schema, table, connection, partitionInfo);
            } else if (partitionKey.startsWith(PartitionInfo.LIST_PARTITION)) {
                partitionInfo = new ListPartitionInfo();
                getListPartitionValues(schema, table, connection, partitionInfo);
            } else if (partitionKey.startsWith(PartitionInfo.HASH_PARTITION)) {
                partitionInfo = new HashPartitionInfo();
                getHashPartitionValues(schema, table, connection, partitionInfo);
            } else {
                throw new IllegalStateException("Unknown partition type: " + partitionKey);
            }
            partitionInfo.setPartitionTable(table);
            partitionInfo.setParentTable(parentTable);
            partitionInfoList.add(partitionInfo);
        }
        return partitionInfoList;
    }

    private void getRangePartitionValues(String schema, String table, Connection connection,
                                         PartitionInfo partitionInfo) {
        if (!(partitionInfo instanceof RangePartitionInfo)) {
            return;
        }
        partitionInfo.setPartitionTable(table);
        String rangeExpr = null;
        String subPartitionKey = null;
        try (Statement statement = connection.createStatement();
             ResultSet rst = statement.executeQuery(
                     String.format(PostgresSqlConstants.GET_PARTITION_EXPR, schema, table))) {
            if (rst.next()) {
                subPartitionKey = rst.getString(1);
                rangeExpr = rst.getString(2);
            }
            partitionInfo.setPartitionKey(subPartitionKey);

            if (rangeExpr != null) {
                // 16 is length of "FOR VALUES FROM "
                rangeExpr = rangeExpr.trim().substring(16);
                // idex of "TO"
                int toIdx = rangeExpr.toUpperCase(Locale.ROOT).indexOf("TO");
                String lowerBound = rangeExpr.substring(0, toIdx).trim();
                String upperBound = rangeExpr.substring(toIdx + 2).trim();
                partitionInfo.setRangeLowerBound(lowerBound);
                partitionInfo.setRangeUpperBound(upperBound);
            }
        } catch (SQLException e) {
            LOGGER.error("get range partition value occurred exception", e);
        }
    }

    private void getListPartitionValues(String schema, String table, Connection connection,
                                        PartitionInfo partitionInfo) {
        if (!(partitionInfo instanceof ListPartitionInfo)) {
            return;
        }
        partitionInfo.setPartitionTable(table);
        String listExpr = null;
        String subPartitionKey = null;
        try (Statement statement = connection.createStatement();
             ResultSet rst = statement.executeQuery(
                     String.format(PostgresSqlConstants.GET_PARTITION_EXPR, schema, table))) {
            if (rst.next()) {
                subPartitionKey = rst.getString(1);
                listExpr = rst.getString(2);
            }
            partitionInfo.setPartitionKey(subPartitionKey);

            if (listExpr != null) {
                // 14 is length of "FOR VALUES IN "
                listExpr = listExpr.trim().substring(14);
                partitionInfo.setListPartitionValue(listExpr);
            }
        } catch (SQLException e) {
            LOGGER.error("get list partition value occurred exception", e);
        }
    }

    private void getHashPartitionValues(String schema, String table, Connection connection,
                                        PartitionInfo partitionInfo) {
        if (!(partitionInfo instanceof HashPartitionInfo)) {
            return;
        }
        partitionInfo.setPartitionTable(table);
        String hashExpr = null;
        String subPartitionKey = null;
        try (Statement statement = connection.createStatement();
             ResultSet rst = statement.executeQuery(
                     String.format(PostgresSqlConstants.GET_PARTITION_EXPR, schema, table))) {
            if (rst.next()) {
                subPartitionKey = rst.getString(1);
                hashExpr = rst.getString(2);
            }
            partitionInfo.setPartitionKey(subPartitionKey);

            if (hashExpr != null) {
                // 16 is length of "FOR VALUES WITH "
                hashExpr = hashExpr.trim().substring(16);
                partitionInfo.setListPartitionValue(hashExpr);
            }
        } catch (SQLException e) {
            LOGGER.error("get hash partition value occurred exception", e);
        }
    }

    private List<String> getChildTables(String schemaName, String tableName, Connection connection) {
        List<String> childs = new ArrayList<>();
        try (Statement stmt = connection.createStatement();
             ResultSet rst = stmt.executeQuery(String.format(
                     PostgresSqlConstants.GET_CHILD_TABLE, schemaName, tableName))) {
            while (rst.next()) {
                childs.add(rst.getString(1));
            }
        } catch (SQLException e) {
            LOGGER.error("get child of table {}.{} failed", schemaName, tableName, e);
        }
        return childs;
    }

    private String getPartitionKey(String schemaName, String tableName, Connection connection) {
        String partitionKey = "";
        try (Statement stmt = connection.createStatement();
             ResultSet rst = stmt.executeQuery(
                     String.format(PostgresSqlConstants.GET_PARTITION_KEY, schemaName, tableName))) {
            if (rst.next()) {
                partitionKey = rst.getString(1);
            }
            if (partitionKey != null && partitionKey.trim().toUpperCase().startsWith(PartitionInfo.LIST_PARTITION)) {
                return removeListPartitionOps(partitionKey.trim().toUpperCase());
            }
        } catch (SQLException e) {
            LOGGER.error("get table partition key occurred SQLException.", e);
        }
        return partitionKey;
    }

    private String removeListPartitionOps(String partitionKey) {
        String[] res = partitionKey.replaceAll("\\s+", " ").split(" ");
        if (res.length <= 1) {
            return partitionKey;
        }
        if (res[res.length - 1].endsWith("_OPS)")) {
            res[res.length - 1] = ")";
        }
        return String.join("", res);
    }

    @Override
    protected String getIsolationSql() {
        return PostgresSqlConstants.SET_SNAPSHOT_SQL;
    }

    @Override
    protected void lockTable(Table table, Connection conn) throws SQLException {
        try (Statement statement = conn.createStatement()) {
            String schemaName = table.getSchemaName();
            String tableName = table.getTableName();
            String lockStatement = String.format(PostgresSqlConstants.SET_TABLE_SNAPSHOT_SQL, schemaName, tableName);
            statement.execute(lockStatement);
        }
    }

    @Override
    protected String getQueryWithLock(Table table, List<Column> columns, Connection conn) {
        List<String> columnNames = columns.stream().map(column -> {
            String name = DatabaseUtils.formatObjName(column.getName());
            if (PostgresColumnType.isGeometryTypes(column.getTypeName())) {
                return "ST_AsText(" + name + ") AS " + name; // PostgreSQL 几何类型转换语法
            }
            return name;
        }).collect(Collectors.toList());
        String queryDataSql = PostgresSqlConstants.QUERY_WITH_LOCK_SQL;;
        List<String> childs = getChildTables(table.getSchemaName(), table.getTableName(), conn);
        if (childs.size() > 0) {
            if (!table.isPartition()) {
                queryDataSql = PostgresSqlConstants.QUERY_PARENT_WITH_LOCK_SQL;
            }
        }
        return String.format(queryDataSql,
                String.join(CommonConstants.DELIMITER, columnNames),
                DatabaseUtils.formatObjName(table.getSchemaName()),
                DatabaseUtils.formatObjName(table.getTableName()));
    }

    @Override
    protected String getSnapShotPoint(Connection conn) {
        return getCurrentServerVersion(conn) < getServerVersionNum(PostgresSqlConstants.PG_SERVER_V10)
                ? PostgresSqlConstants.GET_XLOG_LOCATION_OLD : PostgresSqlConstants.GET_XLOG_LOCATION_NEW;
    }

    @Override
    protected String getQueryObjectSql(String objectType, Connection conn) throws IllegalArgumentException {
        switch (objectType) {
            case "view":
                return PostgresSqlConstants.QUERY_VIEW_SQL;
            case "function":
                return PostgresSqlConstants.QUERY_FUNCTION_SQL;
            case "trigger":
                return PostgresSqlConstants.QUERY_TRIGGER_SQL;
            case "procedure":
                return PostgresSqlConstants.QUERY_PROCEDURE_SQL;
            case "sequence":
                return PostgresSqlConstants.QUERY_SEQUENCE_SQL;
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
            // postgresql 默认1
            cacheSize = cacheSize == 0 ? 1 : cacheSize;
            long increment = rs.getLong("increment");
            increment = increment == 0 ? 1 : increment;
            long startValue = rs.getLong("startValue");
            startValue = startValue == 0 ? (increment > 0 ? 1 : -1) : startValue;
            boolean isCycling = rs.getBoolean("isCycling");
            long currentValue = rs.getLong("currentValue");
            if (rs.wasNull()) {
                return String.format(Locale.ROOT,
                        "CREATE SEQUENCE IF NOT EXISTS %s START WITH %d INCREMENT BY %d MINVALUE %d MAXVALUE %d %s CACHE %d; ",
                        rs.getString("name"), startValue, increment, minValue, maxValue,
                        isCycling ? "CYCLE" : "NOCYCLE", cacheSize);
            } else {
                return String.format(Locale.ROOT,
                        "CREATE SEQUENCE IF NOT EXISTS %s START WITH %d INCREMENT BY %d MINVALUE %d MAXVALUE %d %s CACHE %d; "
                                + "SELECT setval('%s', %d);", rs.getString("name"), startValue, increment, minValue, maxValue,
                        isCycling ? "CYCLE" : "NOCYCLE", cacheSize, rs.getString("name"), currentValue);
            }
        } else if (TaskTypeEnum.VIEW.getTaskType().equalsIgnoreCase(objectType)) {
            return String.format(Locale.ROOT, "CREATE VIEW %s AS %s", rs.getString("name"), rs.getString("definition"));
        }
        return rs.getString("definition");
    }


    private long[] getDataTypeBounds(int systemTypeId) {
        switch (systemTypeId) {
            case 23: // int
                return new long[] {-2147483648L, 2147483647L};
            case 21: // smallint
                return new long[] {-32768L, 32767L};
            case 16: // tinyint
                return new long[] {0L, 255L};
            case 20: // bigint
                return new long[] {-9223372036854775808L, 9223372036854775807L};
            default:
                throw new IllegalArgumentException("Unsupported data type ID: " + systemTypeId);
        }
    }

    @Override
    protected String getQueryIndexSql(String schema) {
        return String.format(PostgresSqlConstants.QUERY_INDEX_SQL, schema);
    }

    @Override
    protected TableIndex getTableIndex(Connection conn, ResultSet rs) throws SQLException {
        TableIndex tableIndex = new TableIndex(rs);
        tableIndex.setIndexprs(rs.getString("index_expression"));
        long objectId = rs.getLong("object_id");
        List<String> indexCols = new ArrayList<>();
        try (Statement stmt = conn.createStatement();
            ResultSet colRs = stmt.executeQuery(
                String.format(PostgresSqlConstants.QUERY_INDEX_COL_SQL, objectId,
                    tableIndex.getIndexName()))) {
            while (colRs.next()) {
                indexCols.add(colRs.getString("column_name"));
            }
            tableIndex.setColumnName(String.join(CommonConstants.DELIMITER, indexCols));
        }
        return tableIndex;
    }

    @Override
    protected String getQueryPkSql() {
        return PostgresSqlConstants.QUERY_PRIMARY_KEY_SQL;
    }

    @Override
    protected String getQueryFkSql(String schema) {
        return String.format(PostgresSqlConstants.QUERY_FOREIGN_KEY_SQL, schema);
    }
}
