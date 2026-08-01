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

package org.full.migration.source.opengauss;

import lombok.EqualsAndHashCode;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.full.migration.constants.CommonConstants;
import org.full.migration.constants.OpenGaussConstants;
import org.full.migration.coordinator.ProgressTracker;
import org.full.migration.coordinator.QueueManager;
import org.full.migration.enums.SqlCompatibilityEnum;
import org.full.migration.jdbc.OpenGaussConnection;
import org.full.migration.model.object.DbObject;
import org.full.migration.model.PostgresCustomTypeMeta;
import org.full.migration.model.TaskTypeEnum;
import org.full.migration.model.config.GlobalConfig;
import org.full.migration.model.object.Procedure;
import org.full.migration.model.object.Sequence;
import org.full.migration.model.object.View;
import org.full.migration.model.table.Column;
import org.full.migration.model.table.GenerateInfo;
import org.full.migration.model.table.Table;
import org.full.migration.model.table.OpenGaussPartitionDefinition;
import org.full.migration.model.table.TableIndex;
import org.full.migration.model.table.TableMeta;
import org.full.migration.source.SourceDatabase;
import org.full.migration.translator.PostgresColumnType;
import org.full.migration.translator.PostgresqlFuncTranslator;
import org.full.migration.utils.DatabaseUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.Optional;
import java.util.HashMap;
import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * OpenGaussSource
 *
 * @since 2025-06-24
 */
@EqualsAndHashCode(callSuper = true)
public abstract class OpenGaussSource extends SourceDatabase {
    private static final Logger LOGGER = LoggerFactory.getLogger(OpenGaussSource.class);
    private static final String LINESEP = System.lineSeparator();

    public OpenGaussSource(GlobalConfig globalConfig, SqlCompatibilityEnum sqlCompatibility) {
        super(globalConfig);
        this.connection = new OpenGaussConnection(sqlCompatibility);
    }

    /**
     * initPublication
     *
     * @param conn
     * @param migraTableNames
     */
    @Override
    protected void initPublication(Connection conn, List<String> migraTableNames) {
        if (CollectionUtils.isEmpty(migraTableNames)) {
            LOGGER.warn("No table names provided for creating the publication.");
            return;
        }

        String migraTableString = migraTableNames.stream()
                .map(t -> DatabaseUtils.formatObjName(t))
                .collect(Collectors.joining(", "));

        String createPublicationStmt = String.format(OpenGaussConstants.CREATE_PUBLICATION, migraTableString);
        try (Statement stmt = conn.createStatement()) {
            try (ResultSet rs = stmt.executeQuery(OpenGaussConstants.SELECT_PUBLICATION)) {
                if (rs.next() && rs.getLong(1) != 0L) {
                    LOGGER.warn("A logical publication named dbz_publication is already active, dropping it.");
                    stmt.execute(OpenGaussConstants.DROP_PUBLICATION);
                }
            } catch (SQLException e) {
                LOGGER.error("Failed to check the existence of 'dbz_publication'. Error: {}", e.getMessage());
                return;
            }

            LOGGER.info("Creating new publication 'dbz_publication' for plugin '{}'", sourceConfig.getPluginName());

            if (migraTableString.isEmpty()) {
                LOGGER.warn("No valid tables provided for the publication.");
            } else {
                LOGGER.info("Creating publication with statement: '{}'", createPublicationStmt);
                try {
                    stmt.execute(createPublicationStmt);
                } catch (SQLException e) {
                    LOGGER.error("Unable to create filtered publication dbz_publication. Error: {}", e.getMessage());
                }
            }
        } catch (SQLException e) {
            LOGGER.error("Failed to create or manage publication 'dbz_publication'. Error: {}", e.getMessage());
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
             ResultSet rs = stmt.executeQuery(String.format(OpenGaussConstants.PG_CREATE_LOGICAL_REPLICATION_SLOT,
                     escapeSqlLiteral(sourceConfig.getSlotName()),
                     escapeSqlLiteral(sourceConfig.getPluginName())))) {
            LOGGER.info("create logical replication slot {} success.", sourceConfig.getSlotName());
        } catch (SQLException e) {
            LOGGER.warn("fail to create logical replication slot, error message:{}.", e.getMessage());
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
                     OpenGaussConstants.PG_GET_LOGICAL_REPLICATION_SLOT,
                     escapeSqlLiteral(sourceConfig.getSlotName())))) {
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
                stmt.executeQuery(String.format(OpenGaussConstants.PG_DROP_LOGICAL_REPLICATION_SLOT,
                        escapeSqlLiteral(sourceConfig.getSlotName())));
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
        try (Connection conn = connection.getConnection(sourceConfig.getDbConn())) {
            if (!table.isHasPrimaryKey()) {
                String sql = String.format(OpenGaussConstants.PG_SET_TABLE_REPLICA_IDNTITY_FULL,
                        DatabaseUtils.formatObjName(table.getSchemaName()),
                        DatabaseUtils.formatObjName(table.getTableName()));
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
        String queryTableSql = String.format(getQueryTableSql(), escapeSqlLiteral(schema));
        try (Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(queryTableSql)) {
            while (rs.next()) {
                String tableName = rs.getString("tablename");
                Table table = new Table(sourceConfig.getDbConn().getDatabase(), schema, tableName);
                table.setTargetSchemaName(sourceConfig.getSchemaMappings().get(schema));
                table.setTotalTableSize(rs.getLong("totalTableSize"));
                table.setRowCount(rs.getInt("tablerows"));
                table.setPartition(rs.getBoolean("isPartitioned"));
                table.setSubPartition(rs.getBoolean("isSubPartitioned"));
                table.setHasPrimaryKey(rs.getBoolean("hasPrimaryKey"));
                table.setHasSegment(rs.getBoolean("has_segment_on"));
                tables.add(table);
            }
        } catch (SQLException e) {
            LOGGER.error("fail to query table list, error message:{}.", e.getMessage());
        }
        return tables;
    }

    String getQueryTableSql() {
        return OpenGaussConstants.QUERY_TABLE_SQL;
    }

    /**
     * createCustomOrDomainTypesSql
     *
     * @param conn conn
     * @param schema schema
     * @return opengaussCustomTypeMetas
     */
    @Override
    protected List<PostgresCustomTypeMeta> createCustomOrDomainTypesSql(Connection conn, String schema) {
        List<PostgresCustomTypeMeta> opengaussCustomTypeMetas = new ArrayList<>();
        opengaussCustomTypeMetas.addAll(createCompositeTypeSql(conn, schema));
        opengaussCustomTypeMetas.addAll(createEnumTypeSql(conn, schema));
        opengaussCustomTypeMetas.addAll(createDomainTypeSql(conn, schema));
        LOGGER.info("end to read custom or domain types.");
        return opengaussCustomTypeMetas;
    }

    /**
     * createCompositeTypeSql
     *
     * @param conn conn
     * @param schema schema
     * @return opengaussCustomTypeMetas
     */
    private List<PostgresCustomTypeMeta> createCompositeTypeSql(Connection conn, String schema) {
        List<PostgresCustomTypeMeta> opengaussCustomTypeMetas = new ArrayList<>();
        String compositeTypeQuery = OpenGaussConstants.QUERY_COMPOSITE_TYPE_SQL;
        try (PreparedStatement stmt = conn.prepareStatement(compositeTypeQuery)) {
            stmt.setString(1, schema);
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    String compositeTypeName = rs.getString("type_name");
                    if (compositeTypeName.contains(".")) {
                        continue;
                    }
                    String typeDefinition = rs.getString("type_definition");
                    String typeComment = rs.getString("type_comment");

                    if (typeComment != null && !typeComment.isEmpty()) {
                        typeDefinition += "\nCOMMENT ON TYPE " + schema + "." + compositeTypeName +
                                " IS '" + typeComment.replace("'", "''") + "';";
                    }

                    opengaussCustomTypeMetas.add(new PostgresCustomTypeMeta(schema, compositeTypeName, typeDefinition));
                }
                return opengaussCustomTypeMetas;
            }
        } catch (SQLException e) {
            LOGGER.error("Failed to query composite types for schema {}: {}", schema, e.getMessage());
        }
        return opengaussCustomTypeMetas;
    }

    /**
     * createEnumTypeSql
     *
     * @param conn conn
     * @param schema schema
     * @return opengaussCustomTypeMetas
     */
    private List<PostgresCustomTypeMeta> createEnumTypeSql(Connection conn, String schema) {
        List<PostgresCustomTypeMeta> opengaussCustomTypeMetas = new ArrayList<>();
        try (PreparedStatement stmt = conn.prepareStatement(OpenGaussConstants.QUERY_ENUM_TYPE_SQL)) {
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

                    opengaussCustomTypeMetas.add(new PostgresCustomTypeMeta(schema, enumTypeName, sqlBuilder.toString()));
                }
                return opengaussCustomTypeMetas;
            }
        } catch (SQLException e) {
            LOGGER.error("Failed to query enum types for schema {}: {}", schema, e.getMessage());
        }
        return opengaussCustomTypeMetas;
    }

    /**
     * createDomainTypeSql
     *
     * @param conn conn
     * @param schema schema
     * @return opengaussCustomTypeMetas
     */
    private List<PostgresCustomTypeMeta> createDomainTypeSql(Connection conn, String schema) {
        List<PostgresCustomTypeMeta> opengaussCustomTypeMetas = new ArrayList<>();
        try (PreparedStatement stmt = conn.prepareStatement(OpenGaussConstants.QUERY_DOMAIN_TYPE_SQL)) {
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

                    if (notNull) {
                        sqlBuilder.append(" NOT NULL");
                    }
                    if (checkConstraint != null && !checkConstraint.isEmpty()) {
                        sqlBuilder.append(" " + checkConstraint);
                    }
                    sqlBuilder.append(";");

                    if (domainComment != null && !domainComment.isEmpty()) {
                        sqlBuilder.append("\nCOMMENT ON DOMAIN ").append(schema).append(".").append(domainTypeName)
                                .append(" IS '").append(domainComment.replace("'", "''")).append("';");
                    }

                    opengaussCustomTypeMetas.add(new PostgresCustomTypeMeta(schema, domainTypeName, sqlBuilder.toString()));
                }
                return opengaussCustomTypeMetas;
            }
        } catch (SQLException e) {
            LOGGER.error("Failed to query domain types for schema {}: {}", schema, e.getMessage());
        }
        return opengaussCustomTypeMetas;
    }

    /**
     * formatEnumValues
     *
     * @param arrayAggResult arrayAggResult
     * @return enumvalues
     */
    private String formatEnumValues(String arrayAggResult) {
        if (StringUtils.isEmpty(arrayAggResult)) {
            LOGGER.warn("enum_values is empty.");
            return "";
        }
        String withoutBraces = arrayAggResult.replaceAll("[{}]", "");
        String[] values = withoutBraces.split(",");
        return Arrays.stream(values)
                .map(String::trim)
                .map(value -> {
                    if (value.startsWith("\"") && value.endsWith("\"") && value.length() >= 2) {
                        return "'" + value.substring(1, value.length() - 1) + "'";
                    }
                    return "'" + value + "'";
                })
                .collect(Collectors.joining(", "));
    }

    /**
     * IsColumnGenerate
     *
     * @param conn
     * @param schema
     * @param tableName
     * @param column
     * @return IsColumnGenerate
     */
    @Override
    protected boolean IsColumnGenerate(Connection conn, String schema, String tableName, Column column) {
        try (PreparedStatement pstmt = conn.prepareStatement(OpenGaussConstants.Check_COLUMN_IS_GENERATE)) {
            pstmt.setString(1, schema);
            pstmt.setString(2, tableName);
            pstmt.setString(3, column.getName());
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getBoolean(1);
                }
            }
        } catch (SQLException e) {
            LOGGER.error("check if column is generate occurred an exception, schema:{}, table:{}, column:{}",
                    schema, tableName, column.getName(), e);
        }
        return false;
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
        try (PreparedStatement pstmt = conn.prepareStatement(OpenGaussConstants.QUERY_GENERATE_DEFINE_SQL)) {
            pstmt.setString(1, schema);
            pstmt.setString(2, tableName);
            pstmt.setString(3, column);
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    GenerateInfo generateInfo = new GenerateInfo();
                    generateInfo.setName(column);
                    generateInfo.setIsStored(rs.getBoolean("is_stored"));
                    generateInfo.setDefine(rs.getString("computation_expression"));
                    return Optional.of(generateInfo);
                }
            }
        } catch (SQLException e) {
            LOGGER.error("query generate define occurred an exception, schema:{}, table:{}, column:{}",
                    schema, tableName, column, e);
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
        return OpenGaussConstants.QUERY_UNIQUE_CONSTRAINT_SQL;
    }

    /**
     * getQueryCheckConstraint
     *
     * @return checkConstraintSql
     */
    @Override
    protected String getQueryCheckConstraint() {
        return OpenGaussConstants.QUERY_CHECK_CONSTRAINT_SQL;
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
            String colType = getColumnType(column).replace("\"" + table.getSchemaName() + "\"", "\""
                    + table.getTargetSchemaName() + "\"");
            if (PostgresColumnType.isTimesTypes(colType) && !sourceConfig.getIsTimeMigrate()) {
                LOGGER.error("{}.{} has column type {}, don't migrate this table according to the configuration",
                        table.getSchemaName(), table.getTableName(), colType);
                return "";
            }
            String nullType = column.isOptional() ? "" : " NOT NULL ";
            columnDdl.add(String.format("%s %s %s", colName, colType, nullType));
        }
        return columnDdl.toString();
    }

    @Override
    public String getColumnDdl(Table table, List<Column> columns, String targetDatabaseType) {
        return getColumnDdl(table, columns);
    }

    /**
     * getColumnType
     *
     * @param column column
     * @return columnStr
     */
    private String getColumnType(Column column) {
        String typeName = column.getTypeName().split(" ")[0];
        StringBuilder builder = new StringBuilder(typeName);
        if (PostgresColumnType.isTypeWithLength(typeName)) {
            long length = column.getLength();
            Integer scale = column.getScale();
            if (PostgresColumnType.isTimesTypes(typeName)) {
                builder.append("(").append(scale).append(")");
            } else if (PostgresColumnType.isTypesInterval(typeName)) {
                if (column.getIntervalType() != null) {
                    builder.append(" " + column.getIntervalType());
                } else {
                    builder.append("(").append(scale).append(")");
                }
            } else {
                if ((PostgresColumnType.isVarsTypes(typeName) && length != Integer.MAX_VALUE)
                        || (PostgresColumnType.isNumericType(typeName) && length > 0)
                        || (!PostgresColumnType.isVarsTypes(typeName) && !PostgresColumnType.isNumericType(typeName))) {
                    builder.append("(").append(length);
                }
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
            builder.append(" GENERATED ALWAYS AS (")
                    .append(column.getGenerateInfo().getDefine())
                    .append(")")
                    .append(column.getGenerateInfo().getIsStored() ? " STORED " : "VIRTUAL");
        } else {
            if (!PostgresColumnType.isSerialTypes(typeName)
                && column.getDefaultValueExpression() != null) {
                builder.append(" default " + column.getDefaultValueExpression());
            }
        }
        return builder.toString();
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
        return false;
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
                     String.format(OpenGaussConstants.GET_PARENT_TABLE,
                             escapeSqlLiteral(schemaName), escapeSqlLiteral(tableName)))) {
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
    public String getPartitionDdl(Connection conn, String schemaName, String tableName, boolean isSubPartition)
            throws SQLException{
        OpenGaussPartitionDefinition parentInfo = getParentTableInfo(schemaName, tableName, conn);
        if (Objects.isNull(parentInfo)) {
            LOGGER.error("query {}.{}'s partition definition failed.", schemaName, tableName);
        }
        List<OpenGaussPartitionDefinition> firstPartitionDefinitions =
                getFirstPartitionDefinitions(parentInfo.getOid(), tableName, conn);
        Map<String, List<OpenGaussPartitionDefinition>> secondPartitionDefinitions = new HashMap<>();
        String subPartitionKey = "";
        String interval = "";
        if (isSubPartition) {
            List<OpenGaussPartitionDefinition> secondaryInfos = new ArrayList<>();
            for (OpenGaussPartitionDefinition firstPartitionDefinition : firstPartitionDefinitions) {
                secondaryInfos = getSubPartitionDefinitions(firstPartitionDefinition.getOid(), conn);
                secondPartitionDefinitions.put(firstPartitionDefinition.getPartitionName(), secondaryInfos);
            }
            subPartitionKey = " SUBPARTITION BY " + getPartitionkey(
                    getSubpartitionColumn(schemaName, tableName, conn), secondaryInfos.get(0).getPartitionType());
        }
        String partitionKey = getPartitionkey(parentInfo.getPartColumn(), parentInfo.getPartitionType());
        if (parentInfo.isIntervalPartition()) {
            //Secondary partitioning does not support interval partition.
            interval = String.format("INTERVAL ('%s')\n", parentInfo.getInterval()
                    .replaceAll("[{}\"]", "").trim());
        }
        StringBuilder partitionDdl = new StringBuilder("\n PARTITION BY ")
                .append(partitionKey)
                .append(subPartitionKey)
                .append("\n")
                .append(interval)
                .append(getPartitionValue(firstPartitionDefinitions, secondPartitionDefinitions));
        return partitionDdl.toString();
    }

    /**
     * getSubpartitionColumn
     *
     * @param conn conn,schema schema, tableName tableName
     * @return subColumn
     */
    private String getSubpartitionColumn(String schemaName, String tableName, Connection conn) {
        try (PreparedStatement stmt = conn.prepareStatement(OpenGaussConstants.GET_SECONDARY_PARTITION_COLUMN)) {
            stmt.setString(1, schemaName);
            stmt.setString(2, tableName);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getString(1);
                }
            }
        } catch (SQLException e) {
            LOGGER.error("query subpartition column occurred SQLException.", e);
        }
        return "";
    }

    /**
     * getParentTableInfo
     *
     * @param conn conn,schema schema, tableName tableName
     * @return partitionDefinition
     */
    private OpenGaussPartitionDefinition getParentTableInfo(String schemaName, String tableName, Connection conn) {
        try (PreparedStatement stmt = conn.prepareStatement(OpenGaussConstants.GET_PARENT_PARTITION_INFO)) {
            stmt.setString(1, schemaName);
            stmt.setString(2, tableName);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return new OpenGaussPartitionDefinition(
                            rs.getLong("parentid"),
                            rs.getString("partname"),
                            rs.getString("partcolumn"),
                            rs.getString("partstrategy"),
                            rs.getString("parttype"),
                            rs.getString("boundaries"),
                            rs.getString("interval")
                    );
                }
            }
        } catch (SQLException e) {
            LOGGER.error("query partition definition occurred SQLException.", e);
        }
        return new OpenGaussPartitionDefinition();
    }

    /**
     * getPartitionValue
     *
     * @param firstParts
     * @param secondParts
     * @return partitionValue
     */
    private String getPartitionValue(List<OpenGaussPartitionDefinition> firstParts,
                                     Map<String, List<OpenGaussPartitionDefinition>> secondParts) {
        StringBuilder builder = new StringBuilder("( ");
        for (OpenGaussPartitionDefinition firstPart : firstParts) {
            String firstPartName = firstPart.getPartitionName();
            if (firstPart.isRangePartition() || firstPart.isIntervalPartition()) {
                builder.append(getRangePartitionDdl(firstPart));
            } else if (firstPart.isListPartition()) {
                builder.append(getListPartitionDdl(firstPart));
            } else if (firstPart.isHashPartition()) {
                builder.append(getHashPartitionDdl(firstPartName));
            } else {
                LOGGER.error("Unknown partition: {}", firstPart.getPartitionName());
            }

            if (CollectionUtils.isNotEmpty(secondParts.get(firstPartName))) {
                builder.append("(" + LINESEP);
                for (OpenGaussPartitionDefinition secondPart : secondParts.get(firstPartName)) {
                    builder.append(getSubPartitionDdl(secondPart));
                }
                builder.deleteCharAt(builder.length() - 1);
                builder.append(")");
            }
            builder.append(",");
        }
        builder.deleteCharAt(builder.length() - 1);
        builder.append(")");
        return builder.toString();
    }

    /**
     * getHashPartitionDdl
     *
     * @param firstPartName
     * @return HashPartitionDdl
     */
    private String getHashPartitionDdl(String firstPartName) {
        return String.format(" PARTITION \"%s\" ", firstPartName);
    }

    /**
     * getListPartitionDdl
     *
     * @param firstPart
     * @return ListPartitionDdl
     */
    private String getListPartitionDdl(OpenGaussPartitionDefinition firstPart) {
        String boundary = firstPart.getBoundary().replaceAll("[{}\"]", "").trim();
        String[] boundaryStr = boundary.split(",");
        StringBuilder boundaryValues = new StringBuilder();
        for (int i = 0; i < boundaryStr.length; i++) {
            if (i > 0) {
                boundaryValues.append(", ");
            }
            boundaryValues.append("'").append(boundaryStr[i].trim()).append("'");
        }
        if ("\'NULL\'".equalsIgnoreCase(boundaryValues.toString())) {
            return String.format(" PARTITION \"%s\" values (DEFAULT) ", firstPart.getPartitionName());
        } else {
            return String.format(" PARTITION \"%s\" values (%s) ", firstPart.getPartitionName(), boundaryValues.toString());
        }
    }

    /**
     * getRangePartitionDdl
     *
     * @param firstPart
     * @return RangePartitionDdl
     */
    private String getRangePartitionDdl(OpenGaussPartitionDefinition firstPart) {
        String boundary = firstPart.getBoundary().replaceAll("[{}\"]", "").trim();
        if ("NULL".equalsIgnoreCase(boundary)) {
            return String.format(" PARTITION \"%s\" values less than (MAXVALUE) ", firstPart.getPartitionName());
        } else {
            return String.format(" PARTITION \"%s\" values less than ('%s') ", firstPart.getPartitionName(), boundary);
        }
    }

    /**
     * getPartitionkey
     *
     * @param partColumn
     * @param partType
     * @return partitionKey
     */
    private String getPartitionkey(String partColumn, String partType) {
        StringBuilder partitionKey = new StringBuilder("");
        partitionKey.append(partType).append("(").append(partColumn).append(")");
        return partitionKey.toString();
    }

    /**
     * getFirstPartitionDefinitions
     *
     * @param parentid
     * @param tableName
     * @param connection
     * @return partitionDefinitions
     */
    private List<OpenGaussPartitionDefinition> getFirstPartitionDefinitions(
            long parentid, String tableName, Connection connection) {
        List<OpenGaussPartitionDefinition> partitionDefinitions = new ArrayList<>();
        try (PreparedStatement stmt = connection.prepareStatement(OpenGaussConstants.GET_FIRST_PARTITION_INFO)) {
            stmt.setLong(1, parentid);
            stmt.setString(2, tableName);
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    OpenGaussPartitionDefinition partitionDefinition = new OpenGaussPartitionDefinition(
                            rs.getLong("firstOid"),
                            rs.getString("partname"),
                            "",
                            rs.getString("partstrategy"),
                            rs.getString("parttype"),
                            rs.getString("boundaries"),
                            rs.getString("interval")
                    );
                    partitionDefinitions.add(partitionDefinition);
                }
                return partitionDefinitions;
            }
        } catch (SQLException e) {
            LOGGER.error("query first partition information occurred SQLException.", e);
        }
        return partitionDefinitions;
    }

    /**
     * getSubPartitionDefinitions
     *
     * @param firstOid
     * @param connection
     * @return subPartitionDefinitions
     */
    private List<OpenGaussPartitionDefinition> getSubPartitionDefinitions(long firstOid, Connection connection) {
        List<OpenGaussPartitionDefinition> subPartitionDefinitions = new ArrayList<>();
        try (PreparedStatement stmt = connection.prepareStatement(OpenGaussConstants.GET_SECONDARY_PARTITION_INFO)) {
            stmt.setLong(1, firstOid);
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    subPartitionDefinitions.add(new OpenGaussPartitionDefinition(
                            rs.getLong("subOid"),
                            rs.getString("partname"),
                            "",
                            rs.getString("partstrategy"),
                            rs.getString("parttype"),
                            rs.getString("boundaries"),
                            rs.getString("interval")));
                }
                return subPartitionDefinitions;
            }
        } catch (SQLException e) {
            LOGGER.error("query subpartition infomation occurred SQLException.", e);
        }
        return subPartitionDefinitions;
    }

    /**
     * getSubPartitionDdl
     *
     * @param secondPart
     * @return subPartitionDdl
     */
    private String getSubPartitionDdl(OpenGaussPartitionDefinition secondPart) {
        StringBuilder subPartitionDdl = new StringBuilder();
        if (secondPart.isRangePartition() || secondPart.isIntervalPartition()) {
            subPartitionDdl.append(getSubRangePartitionDdl(secondPart));
        } else if (secondPart.isListPartition()) {
            subPartitionDdl.append(getSubListPartitionDdl(secondPart));
        } else if (secondPart.isHashPartition()) {
            subPartitionDdl.append(getSubHashPartitionDdl(secondPart));
        } else {
            LOGGER.error("Unknown subPartition: {}", secondPart.getPartitionName());
        }
        return subPartitionDdl.toString();
    }

    /**
     * getSubRangePartitionDdl
     *
     * @param secondPart
     * @return subRangePartitionDdl
     */
    private String getSubRangePartitionDdl(OpenGaussPartitionDefinition secondPart) {
        String boundary = secondPart.getBoundary().replaceAll("[{}\"]", "").trim();
        String subRangePartitionDdl = "";
        if ("NULL".equalsIgnoreCase(boundary)) {
            subRangePartitionDdl = String.format("SUBPARTITION \"%s\" VALUES LESS THAN (MAXVALUE),",
                    secondPart.getPartitionName());
        } else {
            subRangePartitionDdl = String.format("SUBPARTITION \"%s\" VALUES LESS THAN ('%s'),",
                    secondPart.getPartitionName(), boundary);
        }
        return subRangePartitionDdl;
    }

    /**
     * getSubListPartitionDdl
     *
     * @param secondPart
     * @return subListPartitionDdl
     */
    private String getSubListPartitionDdl(OpenGaussPartitionDefinition secondPart) {
        String[] boundary = secondPart.getBoundary().replaceAll("[{}\"]", "").trim().split(",");
        StringBuilder boundaryValues = new StringBuilder();
        for (int i = 0; i < boundary.length; i++) {
            if (i > 0) {
                boundaryValues.append(", ");
            }
            boundaryValues.append("'").append(boundary[i].trim()).append("'");
        }
        String subRangePartitionDdl = "";
        if ("\'NULL\'".equalsIgnoreCase(boundaryValues.toString())) {
            subRangePartitionDdl = String.format("SUBPARTITION \"%s\" VALUES (DEFAULT),",
                    secondPart.getPartitionName());
        } else {
            subRangePartitionDdl = String.format("SUBPARTITION \"%s\" VALUES (%s),",
                    secondPart.getPartitionName(), boundaryValues.toString());
        }
        return subRangePartitionDdl;
    }

    /**
     * getSubHashPartitionDdl
     *
     * @param secondPart
     * @return subHashPartitionDdl
     */
    private String getSubHashPartitionDdl(OpenGaussPartitionDefinition secondPart) {
        return String.format("SUBPARTITION \"%s\",", secondPart.getPartitionName());
    }

    /**
     * getChildTables
     *
     * @param schemaName
     * @param tableName
     * @param connection
     * @return childTables
     */
    private List<String> getChildTables(String schemaName, String tableName, Connection connection) {
        List<String> childs = new ArrayList<>();
        try (Statement stmt = connection.createStatement();
             ResultSet rst = stmt.executeQuery(String.format(
                     OpenGaussConstants.GET_CHILD_TABLE, schemaName, tableName))) {
            while (rst.next()) {
                childs.add(rst.getString(1));
            }
        } catch (SQLException e) {
            LOGGER.error("get child of table {}.{} failed", schemaName, tableName, e);
        }
        return childs;
    }

    @Override
    protected String getIsolationSql() {
        return OpenGaussConstants.SET_SNAPSHOT_SQL;
    }

    @Override
    protected void lockTable(Table table, Connection conn) throws SQLException {
        try (Statement statement = conn.createStatement()) {
            statement.execute(String.format(OpenGaussConstants.SET_TABLE_SNAPSHOT_SQL,
                    DatabaseUtils.formatObjName(table.getSchemaName()),
                    DatabaseUtils.formatObjName(table.getTableName())));
        }
    }

    @Override
    protected String getQueryWithLock(Table table, List<Column> columns, Connection conn) {
        List<String> columnNames = columns.stream().map(column -> {
            String name = DatabaseUtils.formatObjName(column.getName());
            if (PostgresColumnType.isGeometryTypes(column.getTypeName())) {
                return "ST_AsText(" + name + ") AS " + name;
            }
            return name;
        }).collect(Collectors.toList());
        String queryDataSql = OpenGaussConstants.QUERY_FROM_TABLE_SQL;;
        List<String> childs = getChildTables(table.getSchemaName(), table.getTableName(), conn);
        if (childs.size() > 0) {
            queryDataSql = OpenGaussConstants.QUERY_FROM_PARENT_SQL;
        }
        return String.format(queryDataSql,
                String.join(CommonConstants.DELIMITER, columnNames),
                DatabaseUtils.formatObjName(table.getSchemaName()),
                DatabaseUtils.formatObjName(table.getTableName()));
    }

    @Override
    protected String getSnapShotPoint(Connection conn) {
        return OpenGaussConstants.GET_XLOG_LOCATION_OLD;
    }

    @Override
    protected String getQueryObjectSql(String objectType) throws IllegalArgumentException {
        TaskTypeEnum taskTypeEnum = TaskTypeEnum.getTaskTypeEnum(objectType);

        switch (taskTypeEnum) {
            case FUNCTION:
                return OpenGaussConstants.QUERY_FUNCTION_SQL;
            case TRIGGER:
                return OpenGaussConstants.QUERY_TRIGGER_SQL;
            default:
                LOGGER.error(
                        "objectType {} is invalid, please check the object of migration in [view, function, trigger, "
                                + "procedure, sequence]", objectType);
                throw new IllegalArgumentException(objectType + " is an unsupported type.");
        }
    }

    @Override
    protected String convertDefinition(String objectType, ResultSet rs) throws SQLException {
        if (TaskTypeEnum.TRIGGER.getTaskType().equalsIgnoreCase(objectType)) {
            return rs.getString("definition").replaceAll("DEFINER\\s*=\\s*\\w+\\s*", "");
        } else {
            return rs.getString("definition");
        }
    }

    @Override
    protected String getQueryIndexSql(String schema) {
        return String.format(OpenGaussConstants.QUERY_INDEX_SQL, escapeSqlLiteral(schema));
    }

    @Override
    protected TableIndex getTableIndex(Connection conn, ResultSet rs) throws SQLException {
        TableIndex tableIndex = new TableIndex(rs);
        tableIndex.setIndexRange(rs.getString("index_type"));
        tableIndex.setIndexprs(rs.getString("index_expression"));
        long objectId = rs.getLong("object_id");
        List<String> indexCols = new ArrayList<>();
        try (Statement stmt = conn.createStatement();
            ResultSet colRs = stmt.executeQuery(
                String.format(OpenGaussConstants.QUERY_INDEX_COL_SQL, objectId,
                    escapeSqlLiteral(tableIndex.getIndexName())))) {
            while (colRs.next()) {
                indexCols.add(colRs.getString("column_name"));
            }
            tableIndex.setColumnName(String.join(CommonConstants.DELIMITER, indexCols));
        }
        return tableIndex;
    }

    @Override
    protected void confirmUniqueConstraint(Connection conn, String schema, TableIndex tableIndex) throws SQLException {
        if (tableIndex.isUnique()) {
            try (Statement stmt = conn.createStatement();
                 ResultSet constraintRs = stmt.executeQuery(String.format(OpenGaussConstants.QUERY_CONSTRAINTS_SQL,
                         escapeSqlLiteral(schema), escapeSqlLiteral(tableIndex.getIndexName())))) {
                while (constraintRs.next()) {
                    tableIndex.setConstraint(constraintRs.getInt(1) > 0);
                }
            }
        }
    }

    @Override
    protected String getQueryPkSql() {
        return OpenGaussConstants.QUERY_PRIMARY_KEY_SQL;
    }

    @Override
    protected String getQueryFkSql(String schema) {
        return String.format(OpenGaussConstants.QUERY_FOREIGN_KEY_SQL, escapeSqlLiteral(schema));
    }

    @Override
    public void readTableConstruct() {
        try (Connection conn = connection.getConnection(sourceConfig.getDbConn())) {
            while (!QueueManager.getInstance().isQueuePollEnd(QueueManager.TABLE_QUEUE)) {
                Table table = (Table) QueueManager.getInstance().pollQueue(QueueManager.TABLE_QUEUE);
                if (table == null) {
                    continue;
                }
                String schema = table.getSchemaName();
                String tableName = table.getTableName();
                LOGGER.info("Start to read metadata of {}.{}.", schema, tableName);
                DatabaseMetaData metaData = conn.getMetaData();
                ResultSet columnMetadata = metaData.getColumns(table.getCatalogName(), schema, tableName, null);
                List<Column> columns = new ArrayList<>();
                while (columnMetadata.next()) {
                    sourceTableService.readTableColumn(columnMetadata).ifPresent(column -> {
                        Optional<GenerateInfo> generateInfoOptional = getGeneratedDefine(conn, schema, tableName,
                                column.getName());
                        if (IsColumnGenerate(conn, schema, tableName, column) && generateInfoOptional.isPresent()) {
                            column.setGenerated(true);
                            column.setGenerateInfo(generateInfoOptional.get());
                        }
                        columns.add(column);
                    });
                }

                String inheritsDdl = null;
                String parents = getParentTables(conn, table);
                if (!(StringUtils.isEmpty(parents))) {
                    String[] parentArray = parents.split(",");
                    StringBuilder quoted = new StringBuilder();
                    for (int i = 0; i < parentArray.length; i++) {
                        if (i > 0) {
                            quoted.append(",");
                        }
                        quoted.append(DatabaseUtils.formatObjName(parentArray[i].trim()));
                    }
                    inheritsDdl = String.format(" Inherits (%s)", quoted.toString());
                }
                Optional<String> createTableSqlOptional = generateTableDefinition(conn, table, inheritsDdl);
                if (createTableSqlOptional.isPresent()) {
                    QueueManager.getInstance().putToQueue(QueueManager.SOURCE_TABLE_META_QUEUE,
                            new TableMeta(table, createTableSqlOptional.get(), columns, parents));
                }
            }
        } catch (Exception e) {
            LOGGER.error("Failed to read table metadata", e);
        }
    }

    private Optional<String> generateTableDefinition(
            Connection connection, Table table, String inheritsDdl) throws SQLException {
        String schemaName = table.getSchemaName();
        String tableName = table.getTableName();
        Optional<String> selectResult = selectPgGetTableDef(connection, schemaName, tableName);
        if (selectResult.isEmpty()) {
            return Optional.empty();
        }

        String createTableSql = null;
        String tempSql = null;
        List<String> commentSqls = new ArrayList<>();
        for (String sql : selectResult.get().split(";")) {
            tempSql = sql.trim();
            if (tempSql.startsWith("CREATE TABLE")
                    || tempSql.startsWith("CREATE GLOBAL TEMPORARY TABLE")
                    || tempSql.startsWith("CREATE UNLOGGED TABLE")) {
                createTableSql = sql;
                continue;
            }
            if (tempSql.startsWith("COMMENT ON COLUMN") || tempSql.startsWith("COMMENT ON TABLE")) {
                commentSqls.add(sql);
            }
        }

        if (StringUtils.isEmpty(createTableSql)) {
            LOGGER.error("Select create table SQL is null, table: {}.{}", schemaName, tableName);
            return Optional.empty();
        }

        String targetSchema = table.getTargetSchemaName();
        createTableSql = filterDefaultNextval(createTableSql);
        createTableSql = filterForeignKey(createTableSql);
        createTableSql = filterEnumSet(createTableSql);
        createTableSql = combineInheritsDdl(createTableSql, inheritsDdl);
        createTableSql = createTableSql.replaceAll(schemaName + "\\.", targetSchema + ".");

        StringBuilder tableDefinitionSql = new StringBuilder("SET search_path = ")
                .append(DatabaseUtils.formatObjName(targetSchema)).append(";");
        tableDefinitionSql.append(createTableSql).append(";");
        if (!commentSqls.isEmpty()) {
            tableDefinitionSql.append(String.join(";", commentSqls)).append(";");
        }
        return Optional.of(tableDefinitionSql.toString());
    }

    String filterEnumSet(String createTableSql) {
        return createTableSql;
    }

    private String filterDefaultNextval(String createTableSql) {
        return createTableSql.replaceAll("DEFAULT nextval\\('\\w+\\.?\\w+'::regclass\\)", "");
    }

    private String filterForeignKey(String createTableSql) {
        String[] lines = createTableSql.split("\n");
        String resultSql = lines[0];
        String preLine = "";
        String line = "";
        for (int i = 1; i < lines.length; i++) {
            line = lines[i];
            if (line.contains("CONSTRAINT") && line.contains("FOREIGN KEY")) {
                if (!line.endsWith(",") && preLine.endsWith(",")) {
                    preLine = preLine.substring(0, preLine.length() - 1);
                }
            } else {
                resultSql = resultSql.concat(preLine).concat("\n");
                preLine = line;
            }

            if (i == lines.length - 1) {
                resultSql = resultSql.concat(preLine);
            }
        }
        return resultSql;
    }

    private String combineInheritsDdl(String createTableSql, String inheritsDdl) {
        if (StringUtils.isEmpty(inheritsDdl)) {
            return createTableSql;
        }

        int index = getIndexBeforeTableOptions(createTableSql);
        if (index == -1) {
            return createTableSql;
        }
        return createTableSql.substring(0, index + 1) + inheritsDdl + createTableSql.substring(index + 1);
    }

    private int getIndexBeforeTableOptions(String sql) {
        if (sql == null || sql.trim().isEmpty()) {
            return -1;
        }
        int start = sql.indexOf('(');
        if (start == -1) {
            return -1;
        }

        int depth = 0;
        int end = -1;
        for (int i = start; i < sql.length(); i++) {
            char c = sql.charAt(i);
            if (c == '(') {
                depth++;
                continue;
            }
            if (c == ')') {
                depth--;

                if (depth == 0) {
                    end = i;
                    break;
                }
            }
        }
        return end;
    }

    private Optional<String> selectPgGetTableDef(Connection connection, String schema, String table)
            throws SQLException {
        String sqlResult = null;
        String fullName = DatabaseUtils.formatObjName(schema) + "." + DatabaseUtils.formatObjName(table);
        try (PreparedStatement preStatement = connection.prepareStatement(OpenGaussConstants.SELECT_PG_GET_TABLE_DEF)) {
            preStatement.setString(1, fullName);
            try (ResultSet resultSet = preStatement.executeQuery()) {
                if (resultSet.next()) {
                    sqlResult = resultSet.getString(1);
                }
            }
        } catch (SQLException e) {
            if (e.getMessage().contains("Not a ordinary table or foreign table.")) {
                LOGGER.warn("Select table is not a ordinary table or foreign table, table: {}", fullName);
                return Optional.empty();
            } else {
                throw e;
            }
        }

        if (StringUtils.isEmpty(sqlResult)) {
            LOGGER.error("Select table definition is empty, table: {}", fullName);
            return Optional.empty();
        }
        return Optional.of(sqlResult);
    }

    @Override
    public void readObjects(String objectType, String schema) {
        TaskTypeEnum taskTypeEnum = TaskTypeEnum.getTaskTypeEnum(objectType);
        switch (taskTypeEnum) {
            case FUNCTION:
            case TRIGGER:
                super.readObjects(objectType, schema);
                break;
            case VIEW:
                readView(schema);
                break;
            case SEQUENCE:
                readSequence(schema);
                break;
            case PROCEDURE:
                readProcedure(schema);
                break;
            default:
                LOGGER.error("Object type '{}' is invalid, please check the object of migration in [view, function, "
                        + "trigger, procedure, sequence]", objectType);
                throw new IllegalArgumentException("Object type '" + objectType + "' is an unsupported type.");
        }
    }

    private void readProcedure(String schema) {
        try (Connection connection = this.connection.getConnection(sourceConfig.getDbConn())) {
            List<Procedure> procedureList = searchProcedure(connection, schema);
            searchProcedureDefinition(connection, procedureList);

            for (Procedure procedure : procedureList) {
                String name = procedure.getName();
                LOGGER.info("Read procedure: {}.{}", schema, name);
                DbObject dbObject = new DbObject();
                dbObject.setSchema(schema);
                dbObject.setName(name);
                dbObject.setDefinition(procedure.getDefinition());

                QueueManager.getInstance().putToQueue(QueueManager.OBJECT_QUEUE, dbObject);
                if (isDumpJson) {
                    ProgressTracker.getInstance().putProgressMap(schema, name);
                }
            }
            if (isDumpJson) {
                ProgressTracker.getInstance().recordObjectProgress(TaskTypeEnum.PROCEDURE);
            }
        } catch (Exception e) {
            LOGGER.error("Failed to read procedure, schema: {}", schema, e);
        }
        QueueManager.getInstance().setReadFinished(QueueManager.OBJECT_QUEUE, true);
    }

    private List<Procedure> searchProcedure(Connection connection, String schema) throws SQLException {
        List<Procedure> procedures = new ArrayList<>();
        try (PreparedStatement preStatement = connection.prepareStatement(OpenGaussConstants.QUERY_PROCEDURE_SQL)) {
            preStatement.setString(1, schema);
            try (ResultSet resultSet = preStatement.executeQuery()) {
                while (resultSet.next()) {
                    Procedure procedure = new Procedure();
                    procedure.setSchema(schema);
                    procedure.setName(resultSet.getString("name"));
                    procedure.setOId(resultSet.getLong("oid"));
                    procedures.add(procedure);
                }
            }
        }
        return procedures;
    }

    private void searchProcedureDefinition(Connection conn, List<Procedure> procedures)
            throws SQLException {
        try (PreparedStatement preStatement = conn.prepareStatement(OpenGaussConstants.SELECT_PG_GET_FUNCTION_DEF)) {
            for (Procedure procedure : procedures) {
                preStatement.setLong(1, procedure.getOId());
                try (ResultSet resultSet = preStatement.executeQuery()) {
                    while (resultSet.next()) {
                        procedure.setDefinition(resultSet.getString("definition"));
                    }
                }
            }
        }
    }

    private void readView(String schema) {
        try (Connection conn = connection.getConnection(sourceConfig.getDbConn())) {
            List<View> viewList = searchView(conn, schema);
            for (View view : viewList) {
                String name = view.getName();
                LOGGER.info("Read view: {}.{}", schema, name);
                DbObject dbObject = new DbObject();
                dbObject.setSchema(schema);
                dbObject.setName(name);
                dbObject.setDefinition(generateViewDefinition(view));

                QueueManager.getInstance().putToQueue(QueueManager.OBJECT_QUEUE, dbObject);
                if (isDumpJson) {
                    ProgressTracker.getInstance().putProgressMap(schema, name);
                }
            }
            if (isDumpJson) {
                ProgressTracker.getInstance().recordObjectProgress(TaskTypeEnum.VIEW);
            }
        } catch (Exception e) {
            LOGGER.error("Failed to read view, schema: {}", schema, e);
        }
        QueueManager.getInstance().setReadFinished(QueueManager.OBJECT_QUEUE, true);
    }

    private List<View> searchView(Connection conn, String schema) throws SQLException {
        try (PreparedStatement preStatement = conn.prepareStatement(OpenGaussConstants.QUERY_ALL_VIEW_SQL)) {
            preStatement.setString(1, schema);
            try (ResultSet resultSet = preStatement.executeQuery()) {
                List<View> viewList = new ArrayList<>();
                while (resultSet.next()) {
                    View view = new View();
                    view.setSchema(resultSet.getString("schema"));
                    view.setName(resultSet.getString("name"));
                    view.setMaterialized("m".equals(resultSet.getString("view_type")));
                    view.setIncremental(resultSet.getBoolean("isIncremental"));
                    view.setDefinition(resultSet.getString("definition"));
                    viewList.add(view);
                }
                return viewList;
            }
        }
    }

    private String generateViewDefinition(View view) {
        StringBuilder stringBuilder = new StringBuilder("CREATE ");
        if (view.isMaterialized()) {
            if (view.isIncremental()) {
                stringBuilder.append("INCREMENTAL ");
            }
            stringBuilder.append("MATERIALIZED ");
        }
        stringBuilder.append("VIEW ")
                .append(view.getName())
                .append(" AS ")
                .append(view.getDefinition())
                .append(";");
        return stringBuilder.toString();
    }

    private void readSequence(String schema) {
        try (Connection conn = connection.getConnection(sourceConfig.getDbConn());
             Statement statement = conn.createStatement()) {
            List<Sequence> sequenceList = searchSequence(conn, schema);
            selectFromSequence(statement, sequenceList);
            Map<String, Sequence> sequenceUsageMap = searchSequenceOwnedBy(conn, schema);
            sequenceList.forEach(sequence -> {
                Sequence sequenceUsage = sequenceUsageMap.get(sequence.getName());
                if (sequenceUsage != null) {
                    sequence.setOwnedTable(sequenceUsage.getOwnedTable());
                    sequence.setOwnedColumn(sequenceUsage.getOwnedColumn());
                }
            });

            for (Sequence sequence : sequenceList) {
                String name = sequence.getName();
                LOGGER.debug("Read sequence: {}.{}", schema, name);
                DbObject dbObject = new DbObject();
                dbObject.setSchema(schema);
                dbObject.setName(name);
                dbObject.setDefinition(generateSequenceDefinition(sequence));

                QueueManager.getInstance().putToQueue(QueueManager.OBJECT_QUEUE, dbObject);
                if (isDumpJson) {
                    ProgressTracker.getInstance().putProgressMap(schema, name);
                }
            }
            if (isDumpJson) {
                ProgressTracker.getInstance().recordObjectProgress(TaskTypeEnum.SEQUENCE);
            }
        } catch (Exception e) {
            LOGGER.error("Failed to read sequence, schema: {}", schema, e);
        }
        QueueManager.getInstance().setReadFinished(QueueManager.OBJECT_QUEUE, true);
    }

    private String generateSequenceDefinition(Sequence sequence) {
        StringBuilder stringBuilder = new StringBuilder("CREATE ");
        stringBuilder.append(sequence.isLargeSequence() ? "LARGE" : "")
                .append(" SEQUENCE IF NOT EXISTS ").append(sequence.getName())
                .append(" START WITH ").append(sequence.getStartValue())
                .append(" INCREMENT BY ").append(sequence.getIncrementBy())
                .append(" MINVALUE ").append(sequence.getMinValue())
                .append(" MAXVALUE ").append(sequence.getMaxValue())
                .append(sequence.isCycled() ? "CYCLE" : "NOCYCLE")
                .append(" CACHE ").append(sequence.getCacheValue())
                .append(" OWNED BY ").append(sequence.getOwnedBy()).append(";")
                .append(" SELECT setval('").append(sequence.getName()).append("', ")
                .append(sequence.getLastValue()).append(");");

        if (sequence.getOwnedTable() != null && sequence.getOwnedColumn() != null) {
            stringBuilder.append(" ALTER TABLE ").append(sequence.getOwnedTable())
                    .append(" ALTER COLUMN ").append(sequence.getOwnedColumn())
                    .append(" SET DEFAULT nextval('").append(sequence.getName())
                    .append("'::regclass);");
        }
        return stringBuilder.toString();
    }

    private List<Sequence> searchSequence(Connection connection, String schema) throws SQLException {
        List<Sequence> sequenceList = new ArrayList<>();
        try (PreparedStatement preStatement = connection.prepareStatement(OpenGaussConstants.QUERY_ALL_SEQUENCES_SQL)) {
            preStatement.setString(1, schema);
            try (ResultSet resultSet = preStatement.executeQuery()) {
                while (resultSet.next()) {
                    Sequence sequence = new Sequence();
                    sequence.setSchema(schema);
                    sequence.setName(resultSet.getString("relname"));
                    sequence.setLargeSequence("L".equals(resultSet.getString("relkind")));
                    sequenceList.add(sequence);
                }
            }
        }
        return sequenceList;
    }

    private void selectFromSequence(Statement statement, List<Sequence> sequenceList) {
        String selectSequenceSql;
        for (Sequence sequence : sequenceList) {
            selectSequenceSql = String.format(OpenGaussConstants.SELECT_SEQUENCE_SQL,
                    DatabaseUtils.formatObjName(sequence.getSchema()), DatabaseUtils.formatObjName(sequence.getName()));
            try (ResultSet resultSet = statement.executeQuery(selectSequenceSql)) {
                if (resultSet.next()) {
                    sequence.setLastValue(resultSet.getString("last_value"));
                    sequence.setStartValue(resultSet.getString("start_value"));
                    sequence.setIncrementBy(resultSet.getLong("increment_by"));
                    sequence.setMaxValue(resultSet.getString("max_value"));
                    sequence.setMinValue(resultSet.getString("min_value"));
                    sequence.setCacheValue(resultSet.getLong("cache_value"));
                    sequence.setCycled(resultSet.getBoolean("is_cycled"));
                }
            } catch (SQLException e) {
                LOGGER.error("Failed to query sequence info, name: {}.{}", sequence.getSchema(), sequence.getName(), e);
            }
        }
    }

    private static Map<String, Sequence> searchSequenceOwnedBy(Connection conn, String schema)
            throws SQLException {
        Map<String, Sequence> resultMap = new HashMap<>();
        try (PreparedStatement preStatement = conn.prepareStatement(OpenGaussConstants.QUERY_SEQUENCE_USAGE_SQL)) {
            preStatement.setString(1, schema);
            try (ResultSet resultSet = preStatement.executeQuery()) {
                while (resultSet.next()) {
                    Sequence sequence = new Sequence();
                    String sequenceName = resultSet.getString("sequence_name").replaceAll(schema + "\\.", "");
                    sequence.setName(sequenceName);
                    sequence.setOwnedTable(resultSet.getString("table_name"));
                    sequence.setOwnedColumn(resultSet.getString("column_name"));
                    resultMap.put(sequenceName, sequence);
                }
            }
        }
        return resultMap;
    }
}
