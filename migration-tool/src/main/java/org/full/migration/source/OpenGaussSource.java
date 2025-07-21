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
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.full.migration.constants.CommonConstants;
import org.full.migration.constants.OpenGaussConstants;
import org.full.migration.jdbc.OpenGaussConnection;
import org.full.migration.model.PostgresCustomTypeMeta;
import org.full.migration.model.TaskTypeEnum;
import org.full.migration.model.config.GlobalConfig;
import org.full.migration.model.table.Column;
import org.full.migration.model.table.GenerateInfo;
import org.full.migration.model.table.Table;
import org.full.migration.model.table.OpenGaussPartitionDefinition;
import org.full.migration.model.table.TableIndex;
import org.full.migration.translator.PostgresColumnType;
import org.full.migration.translator.PostgresqlFuncTranslator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.Locale;
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
public class OpenGaussSource extends SourceDatabase {
    private static final Logger LOGGER = LoggerFactory.getLogger(OpenGaussSource.class);
    private static final String LINESEP = System.lineSeparator();
    public OpenGaussSource(GlobalConfig globalConfig) {
        super(globalConfig);
        this.connection = new OpenGaussConnection();
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

        String migraTableString = String.join(", ", migraTableNames);

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
                     sourceConfig.getSlotName(), sourceConfig.getPluginName()))) {
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
                     OpenGaussConstants.PG_GET_LOGICAL_REPLICATION_SLOT, sourceConfig.getSlotName()))) {
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
        try (Connection conn = connection.getConnection(sourceConfig.getDbConn())) {
            if (!table.isHasPrimaryKey()) {
                String sql = String.format(OpenGaussConstants.PG_SET_TABLE_REPLICA_IDNTITY_FULL, table.getSchemaName(), table.getTableName());
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
        String queryTableSql = String.format(OpenGaussConstants.QUERY_TABLE_SQL, schema);
        try (Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(queryTableSql)) {
            while (rs.next()) {
                String tableName = rs.getString("tablename");
                Table table = new Table(sourceConfig.getDbConn().getDatabase(), schema, tableName);
                table.setTargetSchemaName(sourceConfig.getSchemaMappings().get(schema));
                table.setAveRowLength(rs.getLong("avgRowLength"));
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
                     String.format(OpenGaussConstants.GET_PARENT_TABLE, schemaName, tableName))) {
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
        return String.format(" PARTITION %s ", firstPartName);
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
            return String.format(" PARTITION %s values (DEFAULT) ", firstPart.getPartitionName());
        } else {
            return String.format(" PARTITION %s values (%s) ", firstPart.getPartitionName(), boundaryValues.toString());
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
            return String.format(" PARTITION %s values less than (MAXVALUE) ", firstPart.getPartitionName());
        } else {
            return String.format(" PARTITION %s values less than ('%s') ", firstPart.getPartitionName(), boundary);
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
            subRangePartitionDdl = String.format("SUBPARTITION %s VALUES LESS THAN (MAXVALUE),",
                    secondPart.getPartitionName());
        } else {
            subRangePartitionDdl = String.format("SUBPARTITION %s VALUES LESS THAN ('%s'),",
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
            subRangePartitionDdl = String.format("SUBPARTITION %s VALUES (DEFAULT),",
                    secondPart.getPartitionName());
        } else {
            subRangePartitionDdl = String.format("SUBPARTITION %s VALUES (%s),",
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
        return String.format("SUBPARTITION %s,", secondPart.getPartitionName());
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
                    table.getSchemaName(), table.getTableName()));
        }
    }

    @Override
    protected String getQueryWithLock(Table table, List<Column> columns, Connection conn) {
        List<String> columnNames = columns.stream().map(column -> {
            String name = column.getName();
            if (PostgresColumnType.isGeometryTypes(column.getTypeName())) {
                return "ST_AsText(" + name + ") AS " + name;
            }
            return name;
        }).collect(Collectors.toList());
        String queryDataSql = OpenGaussConstants.QUERY_WITH_LOCK_SQL;;
        List<String> childs = getChildTables(table.getSchemaName(), table.getTableName(), conn);
        if (childs.size() > 0) {
            queryDataSql = OpenGaussConstants.QUERY_PARENT_WITH_LOCK_SQL;
        }
        return String.format(queryDataSql,
                String.join(CommonConstants.DELIMITER, columnNames),
                table.getSchemaName(),
                table.getTableName());
    }

    @Override
    protected String getSnapShotPoint(Connection conn) {
        return OpenGaussConstants.GET_XLOG_LOCATION_OLD;
    }

    @Override
    protected String getQueryObjectSql(String objectType, Connection conn) throws IllegalArgumentException {
        TaskTypeEnum taskTypeEnum = TaskTypeEnum.getTaskTypeEnum(objectType);

        switch (taskTypeEnum) {
            case VIEW:
                return OpenGaussConstants.QUERY_VIEW_SQL;
            case FUNCTION:
                return OpenGaussConstants.QUERY_FUNCTION_SQL;
            case TRIGGER:
                return OpenGaussConstants.QUERY_TRIGGER_SQL;
            case PROCEDURE:
                return OpenGaussConstants.QUERY_PROCEDURE_SQL;
            case SEQUENCE:
                try (Statement statement = conn.createStatement()) {
                    statement.execute(OpenGaussConstants.CREATE_GET_SEQUENCE_INFO_FUNC);
                } catch (SQLException e) {
                    LOGGER.error("Failed to create function to obtain all sequence information " +
                            "from the specified schema. error message:{}", e.getMessage());
                }
                return OpenGaussConstants.QUERY_SEQUENCE_SQL;
            default:
                LOGGER.error(
                        "objectType {} is invalid, please check the object of migration in [view, function, trigger, "
                                + "procedure, sequence]", objectType);
                throw new IllegalArgumentException(objectType + " is an unsupported type.");
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
            // openGauss 默认1
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
            return String.format(Locale.ROOT, "CREATE VIEW %s AS %s",
                    rs.getString("name"), rs.getString("definition"));
        } else if (TaskTypeEnum.TRIGGER.getTaskType().equalsIgnoreCase(objectType)) {
            return rs.getString("definition").replaceAll("DEFINER\\s*=\\s*\\w+\\s*", "");
        } else {
            return rs.getString("definition");
        }
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
        return String.format(OpenGaussConstants.QUERY_INDEX_SQL, schema);
    }

    @Override
    protected TableIndex getTableIndex(Connection conn, ResultSet rs) throws SQLException {
        TableIndex tableIndex = new TableIndex(rs);
        tableIndex.setIndexprs(rs.getString("index_expression"));
        long objectId = rs.getLong("object_id");
        List<String> indexCols = new ArrayList<>();
        try (Statement stmt = conn.createStatement();
            ResultSet colRs = stmt.executeQuery(
                String.format(OpenGaussConstants.QUERY_INDEX_COL_SQL, objectId,
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
        return OpenGaussConstants.QUERY_PRIMARY_KEY_SQL;
    }

    @Override
    protected String getQueryFkSql(String schema) {
        return String.format(OpenGaussConstants.QUERY_FOREIGN_KEY_SQL, schema);
    }
}
