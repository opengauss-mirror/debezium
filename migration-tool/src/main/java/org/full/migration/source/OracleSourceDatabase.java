/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */

package org.full.migration.source;

import org.full.migration.exception.ErrorCode;
import org.full.migration.exception.MigrationException;
import org.full.migration.exception.TranslatorException;
import org.full.migration.jdbc.OracleConnection;
import org.full.migration.model.PostgresCustomTypeMeta;
import org.full.migration.model.config.GlobalConfig;
import org.full.migration.model.table.Column;
import org.full.migration.model.table.GenerateInfo;
import org.full.migration.model.table.Table;
import org.full.migration.model.table.TableIndex;
import org.full.migration.model.table.TableMeta;
import org.full.migration.source.constraint.ConstraintProcessor;
import org.full.migration.source.partition.OraclePartitionHandler;
import org.full.migration.source.partition.PartitionHandler;
import org.full.migration.translator.Oracle2OgracTranslator;
import org.full.migration.translator.Source2TargetTranslator;
import org.full.migration.translator.TranslatorFactory;
import org.full.migration.utils.DatabaseUtils;
import org.full.migration.utils.MigrationErrorLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Reader;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.full.migration.constants.CommonConstants;
import org.full.migration.constants.OracleSqlConstants;
import org.full.migration.coordinator.QueueManager;

/**
 * OracleSourceDatabase
 * Oracle database source implementation
 *
 * @since 2025-04-18
 */
public class OracleSourceDatabase extends SourceDatabase {
    private static final Logger LOGGER = LoggerFactory.getLogger(OracleSourceDatabase.class);
    // char used to support Oracle-specific syntax for character types, e.g.,
    // varchar2(10 CHAR), varchar(20 BYTE)
    private static final Map<String, String> CHAR_USED_MAP = Map.of("C", "CHAR", "B", "BYTE");
    private static final Oracle2OgracTranslator TRANSLATOR = new Oracle2OgracTranslator();

    private final PartitionHandler partitionHandler = new OraclePartitionHandler();

    /**
     * Constructor
     *
     * @param globalConfig globalConfig
     */
    public OracleSourceDatabase(GlobalConfig globalConfig) {
        super(globalConfig);
        this.connection = new OracleConnection();
    }

    @Override
    protected String getDatabaseType() {
        return "oracle";
    }

    @Override
    protected List<Table> getSchemaAllTables(String schema, Connection conn) {
        List<Table> tables = new ArrayList<>();
        try (Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(OracleSqlConstants.QUERY_TABLE_SQL)) {
            while (rs.next()) {
                String tableName = rs.getString("TableName");
                Table table = new Table(sourceConfig.getDbConn().getDatabase(), schema, tableName);
                table.setTargetSchemaName(sourceConfig.getSchemaMappings().get(schema));
                table.setRowCount(rs.getInt("tableRows"));
                table.setPartition(rs.getBoolean("isPartitioned"));
                table.setSubPartition(false);
                table.setHasPrimaryKey(rs.getBoolean("hasPrimaryKey"));
                tables.add(table);
            }
        } catch (SQLException e) {
            LOGGER.error("fail to query table list, error message:{}.", e.getMessage());
        }
        Map<String, String> tablePkColumn = queryTablePkColumn(conn, schema);
        tables.forEach(table -> {
            table.setPkColumns(tablePkColumn.get(table.getTableName()));
        });
        Map<String, String> tableColumn = queryTableColumn(conn, schema);
        tables.forEach(table -> {
            table.setTbColumns(tableColumn.get(table.getTableName()));
        });
        return tables;
    }

    private Map<String, String> queryTableColumn(Connection conn, String schema) {
        Map<String, String> tableColumn = new HashMap<>();
        try (PreparedStatement ps = conn.prepareStatement(OracleSqlConstants.QUERY_TB_COLUMN_SQL);
                ResultSet resultSet = ps.executeQuery()) {
            while (resultSet.next()) {
                tableColumn.put(resultSet.getString("table_name"), resultSet.getString("tb_columns"));
            }
        } catch (SQLException e) {
            LOGGER.error("query schema table {} column error :{} ", schema, e.getMessage(), e);
        }
        return tableColumn;
    }

    private Map<String, String> queryTablePkColumn(Connection conn, String schema) {
        Map<String, String> tablePkColumn = new HashMap<>();
        try (PreparedStatement ps = conn.prepareStatement(OracleSqlConstants.QUERY_PK_SQL)) {
            ps.setString(1, schema);
            ResultSet resultSet = ps.executeQuery();
            while (resultSet.next()) {
                tablePkColumn.put(resultSet.getString("table_name"), resultSet.getString("pk_columns"));
            }
        } catch (SQLException e) {
            LOGGER.error("query schema table {} pk column error :{} ", schema, e.getMessage(), e);
        }
        return tablePkColumn;
    }

    /**
     * Get all tables under the specified schema
     *
     * @param connection Database connection
     * @param schemaName Schema name
     * @return List of tables
     * @throws SQLException SQL exception
     */
    protected List<Table> getTables(Connection connection, String schemaName) throws SQLException {
        LOGGER.info("Getting tables for schema: {}", schemaName);
        List<Table> tables = new ArrayList<>();
        DatabaseMetaData metaData = connection.getMetaData();

        try (ResultSet rs = metaData.getTables(null, schemaName.toUpperCase(Locale.ROOT), null,
                new String[] { "TABLE" })) {
            while (rs.next()) {
                String tableName = rs.getString("TABLE_NAME");
                String catalogName = rs.getString("TABLE_CAT");
                // DR$索引名$后缀的表名，不处理
                if (tableName.startsWith("DR$")) {
                    continue;
                }
                Table table = new Table();
                table.setCatalogName(catalogName);
                table.setSchemaName(schemaName.toUpperCase(Locale.ROOT));
                table.setTableName(tableName);

                if (sourceConfig != null && sourceConfig.getSchemaMappings() != null) {
                    table.setTargetSchemaName(sourceConfig.getSchemaMappings().get(schemaName));
                }

                tables.add(table);
                LOGGER.debug("Found table: {}.{}", schemaName, tableName);
            }
        } catch (SQLException e) {
            LOGGER.error("Error getting tables for schema {}: {}", schemaName, e.getMessage());
            throw e;
        }

        LOGGER.info("Found {} tables in schema: {}", tables.size(), schemaName);
        return tables;
    }

    /**
     * readTableConstruct
     */
    @Override
    public void readTableConstruct() {
        try (Connection conn = connection.getConnection(sourceConfig.getDbConn())) {
            while (!QueueManager.getInstance().isQueuePollEnd(QueueManager.TABLE_QUEUE)) {
                Table table = (Table) QueueManager.getInstance().pollQueue(QueueManager.TABLE_QUEUE);
                if (table == null) {
                    continue;
                }
                processTableMetadata(conn, table);
            }
        } catch (SQLException e) {
            LOGGER.error("Unable to connect to database {}.{}, please check the status of database and config file",
                    sourceConfig.getDbConn().getHost(), sourceConfig.getDbConn().getDatabase(), e);
        } catch (Exception e) {
            LOGGER.error("Failed to read table metadata", e);
        }
    }

    private void processTableMetadata(Connection conn, Table table) {
        String schema = table.getSchemaName();
        String tableName = table.getTableName();
        LOGGER.info("start to read metadata of {}.{}.", schema, tableName);

        try {
            List<Column> columns = readTableColumns(conn, schema, tableName, table);
            if (columns.isEmpty()) {
                return;
            }

            setCharUsedForColumns(conn, schema, tableName, columns);
            setCommentForColumns(conn, schema, tableName, columns);

            String partitionDdl = getPartitionDdlIfNeeded(conn, schema, tableName, table);
            String parents = getParentTables(conn, table);
            String inheritsDdl = getInheritsDdlIfNeeded(parents);
            String columnsDdl = getColumnDdl(table, columns);

            Optional<String> createTableSqlOptional = generateCreateTableSql(table, columnsDdl, partitionDdl, inheritsDdl);
            if (!createTableSqlOptional.isPresent()) {
                return;
            }
            String finalCreateTableSql = addTableCommentIfNeeded(conn, tableName, createTableSqlOptional.get());
            QueueManager.getInstance()
                    .putToQueue(QueueManager.SOURCE_TABLE_META_QUEUE,
                            new TableMeta(table, finalCreateTableSql, columns, parents));
            LOGGER.info("read table create sql : {}", finalCreateTableSql);
        } catch (SQLException | MigrationException e) {
            LOGGER.error("Failed to read metadata for table {}.{}, error: {}", schema, tableName, e.getMessage());
            MigrationErrorLogger.getInstance().logSqlError(
                    "Table Metadata Reading",
                    schema + "." + tableName,
                    "Reading table metadata",
                    e.getMessage());
        }
    }

    private List<Column> readTableColumns(Connection conn, String schema, String tableName, Table table)
            throws SQLException {
        DatabaseMetaData metaData = conn.getMetaData();
        ResultSet columnMetadata = null;
        List<Column> columns = new ArrayList<>();

        try {
            columnMetadata = metaData.getColumns(table.getCatalogName(), schema, tableName, null);
            while (columnMetadata.next()) {
                sourceTableService.readTableColumn(columnMetadata).ifPresent(column -> {
                    Optional<GenerateInfo> generateInfoOptional = getGeneratedDefine(conn, schema, tableName,
                            column.getName());
                    if (IsColumnGenerate(conn, schema, tableName, column) && generateInfoOptional.isPresent()) {
                        column.setGenerated(true);
                        column.setGenerateInfo(generateInfoOptional.get());
                    } else {
                        column.setGenerated(false);
                    }
                    columns.add(column);
                });
            }
        } finally {
            if (columnMetadata != null) {
                try {
                    columnMetadata.close();
                } catch (SQLException e) {
                    LOGGER.warn("Failed to close ResultSet: {}", e.getMessage());
                }
            }
        }
        return columns;
    }

    private String getPartitionDdlIfNeeded(Connection conn, String schema, String tableName, Table table) throws SQLException {
        if (table.isPartition()) {
            return getPartitionDdl(conn, schema, tableName, table.isSubPartition());
        }
        return null;
    }

    private String getInheritsDdlIfNeeded(String parents) {
        if (!(StringUtils.isEmpty(parents))) {
            return String.format(" Inherits (%s)", parents);
        }
        return null;
    }

    private Optional<String> generateCreateTableSql(Table table, String columnsDdl, String partitionDdl, String inheritsDdl) {
        return sourceTableService.getCreateTableSql(table, columnsDdl, partitionDdl, inheritsDdl);
    }

    /**
     * addTableCommentIfNeeded
     * Add table comment if it exists
     * @param conn      connection
     * @param tableName table name
     * @param createTableSql create table sql
     * @return final create table sql
     */
    private String addTableCommentIfNeeded(Connection conn, String tableName, String createTableSql) throws SQLException {
        Optional<String> tableCommentSqlOptional = buildTableComment(conn, tableName);
        if (tableCommentSqlOptional.isPresent()) {
            return createTableSql + ";" + tableCommentSqlOptional.get();
        }
        return createTableSql;
    }

    private Optional<String> buildTableComment(Connection conn, String tableName) {
        String commentText = null;
        try (PreparedStatement pstmt = conn.prepareStatement(OracleSqlConstants.QUERY_TABLE_COMMENT_SQL)) {
            pstmt.setString(1, tableName);
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    commentText = rs.getString("COMMENTS");
                }
            }
        } catch (SQLException e) {
            LOGGER.error("query table comment occurred an exception, table:{}", tableName, e);
        }
        if (commentText == null || commentText.isEmpty()) {
            return Optional.empty();
        }
        String tableCommentSql = String.format("COMMENT ON TABLE %s IS '%s'", tableName, commentText);
        return Optional.of(tableCommentSql);
    }

    @Override
    protected List<PostgresCustomTypeMeta> createCustomOrDomainTypesSql(Connection conn, String schema) {
        return List.of();
    }

    @Override
    protected Optional<GenerateInfo> getGeneratedDefine(Connection conn, String schema, String tableName,
            String column) {
        try (PreparedStatement pstmt = conn.prepareStatement(OracleSqlConstants.QUERY_GENERATE_DEFINE_SQL)) {
            pstmt.setString(1, tableName);
            pstmt.setString(2, column);
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    if (isVirtualNonXmlColumn(rs)) {
                        return Optional.empty();
                    }
                    GenerateInfo generateInfo = createGenerateInfo(column, rs);
                    return Optional.of(generateInfo);
                }
            }
        } catch (SQLException e) {
            LOGGER.error("Error querying generate define for column {}.{}.{}, error: {}", 
                    schema, tableName, column, e.getMessage());
            MigrationErrorLogger.getInstance().logSqlError(
                    "Generate Define Query",
                    schema + "." + tableName + "." + column,
                    "Querying generate define",
                    e.getMessage());
        }
        return Optional.empty();
    }

    private boolean isVirtualNonXmlColumn(ResultSet rs) throws SQLException {
        String dataType = rs.getString("DATA_TYPE");
        boolean isVirtualColumn = rs.getBoolean("VIRTUAL_COLUMN");
        return (isVirtualColumn && !Objects.equals(dataType, "XMLTYPE"));
    }

    private GenerateInfo createGenerateInfo(String column, ResultSet rs) throws SQLException {
        GenerateInfo generateInfo = new GenerateInfo();
        generateInfo.setName(column);
        generateInfo.setIsStored(true);
        
        // 获取表达式并处理可能的null值
        String expression = getColumnLongValue(rs, "EXPRESSION");
        if (expression == null) {
            LOGGER.warn("Failed to get expression for column: {}", column);
        }
        
        generateInfo.setDefine(expression);
        return generateInfo;
    }

    @Override
    public void processConstraints(Connection conn, String schema, String query, ConstraintProcessor processor,
            String constraintType) throws SQLException {
        try (PreparedStatement pstmt = conn.prepareStatement(query); ResultSet rs = pstmt.executeQuery()) {
            while (rs.next()) {
                String tableName = rs.getString("table_name");
                if (isNotNeedMigraTable(schema, tableName, conn)) {
                    continue;
                }
                String constraintName = rs.getString("constraint_name");
                String constraintValue = constraintType.equalsIgnoreCase("unique")
                        ? rs.getString("columns")
                        : getColumnLongValue(rs, "definition");
                try {
                    String sql = processor.process(tableName, constraintName, constraintValue);
                    QueueManager.getInstance().putToQueue(QueueManager.TABLE_CONSTRAINT_QUEUE, sql);
                } catch (TranslatorException e) {
                    LOGGER.error("fail to process constraint, errorMsg:{}", e.getMessage(), e);
                }
            }
        }
    }

    @Override
    protected String getQueryUniqueConstraint() {
        return OracleSqlConstants.QUERY_UNIQUE_CONSTRAINT_SQL;
    }

    @Override
    protected String getQueryCheckConstraint() {
        return OracleSqlConstants.QUERY_CHECK_CONSTRAINT_SQL;
    }

    @Override
    public String convertToOpenGaussSyntax(String oracleDefinition) {
        return oracleDefinition;
    }

    @Override
    public boolean isGeometryTypes(String typeName) {
        // data x migration non-impl
        return false;
    }

    @Override
    protected boolean IsColumnGenerate(Connection conn, String schema, String tableName, Column column) {
        return true;
    }

    @Override
    public String getPartitionDdl(Connection conn, String schema, String tableName, boolean isSubPartition)
            throws SQLException {
        return partitionHandler.generatePartitionDdl(conn, schema, tableName, isSubPartition);
    }

    @Override
    public String getParentTables(Connection conn, Table table) throws SQLException {
        // Oracle不支持PostgreSQL风格的表继承，返回空字符串
        return "";
    }

    @Override
    public boolean isPartitionChildTable(String schema, String table, Connection connection) {
        return false;
    }

    @Override
    public String getColumnDdl(Table table, List<Column> columns, String targetDatabaseType)
            throws TranslatorException {
        StringBuilder columnDdl = new StringBuilder();
        Source2TargetTranslator translator = TranslatorFactory.getTranslator(getDatabaseType(), targetDatabaseType);
        // 删除虚拟列
        columns.removeIf(column -> !column.isGenerated());
        for (int i = 0; i < columns.size(); i++) {
            Column column = columns.get(i);
            columnDdl.append(column.getName())
                    .append(" ")
                    .append(translator.translateColumnType(table.getTableName(), column).get());

            if (!column.isOptional()) {
                columnDdl.append(" NOT NULL");
            }

            // 处理默认值表达式
            handleDefaultValue(column, columnDdl);
            // 处理注释
            appendColumnComment(column, columnDdl);

            if (i < columns.size() - 1) {
                columnDdl.append(", ");
            }
        }
        return columnDdl.toString();
    }

    private void appendColumnComment(Column column, StringBuilder columnDdl) {
        if (column.getComment() != null) {
            columnDdl.append(" COMMENT '")
                    .append(column.getComment().replace("'", " "))
                    .append("'");
        }
    }

    @Override
    public String getColumnDdl(Table table, List<Column> columns) throws TranslatorException {
        return getColumnDdl(table, columns, "ograc");
    }

    /**
     * Remove comments from string, including multi-line comments, single-line
     * comments, and extra whitespace
     *
     * @param input Input string
     * @return String with comments removed
     */
    private String removeComments(String input) {
        if (input == null) {
            return null;
        }
        String result = input.replaceAll("/\\*[\\s\\S]*?\\*/", "");
        result = result.replaceAll("--.*$", "");
        return result.trim();
    }

    /**
     * Handle default value expression
     *
     * @param column    Column object
     * @param columnDdl Column DDL builder
     */
    private void handleDefaultValue(Column column, StringBuilder columnDdl) {
        if (column.getDefaultValueExpression() != null) {
            String defaultValue = column.getDefaultValueExpression();
            // Skip sequence values to avoid creating non-existent sequences in target
            // database
            if (defaultValue.contains("ISEQ$") && defaultValue.contains(".nextval")) {
                return;
            }
            // Remove comments from default value expression
            defaultValue = removeComments(defaultValue);
            columnDdl.append(" DEFAULT ").append(defaultValue);
        }
    }

    @Override
    protected String getIsolationSql() {
        // Oracle isolation level setting
        return "SET TRANSACTION ISOLATION LEVEL READ COMMITTED";
    }

    @Override
    protected void lockTable(Table table, Connection conn) throws SQLException {
        // oracle no use this function
    }

    @Override
    protected String getQueryWithLock(Table table, List<Column> columns, Connection conn) {
        // oracle no use this function
        return "";
    }

    @Override
    protected String getSnapShotPoint(Connection conn) {
        // TODO 暂不实现
        return "";
    }

    @Override
    protected String getQueryObjectSql(String objectType) throws IllegalArgumentException {
        switch (objectType) {
            case "view":
                return OracleSqlConstants.QUERY_VIEW_SQL;
            case "function":
                return OracleSqlConstants.QUERY_FUNCTION_SQL;
            case "trigger":
                return OracleSqlConstants.QUERY_TRIGGER_SQL;
            case "procedure":
                return OracleSqlConstants.QUERY_PROCEDURE_SQL;
            case "sequence":
                return OracleSqlConstants.QUERY_SEQUENCE_SQL;
            default:
                LOGGER.error(
                        "objectType {} is invalid, please check the object of migration in [view, function, trigger, "
                                + "procedure, sequence]",
                        objectType);
                throw new IllegalArgumentException(objectType + "is an unsupported type.");
        }
    }

    @Override
    protected String convertDefinition(String objectType, ResultSet rs) throws TranslatorException {
        try {
            return switch (objectType.toLowerCase()) {
                case "sequence" -> convertSequenceDefinition(rs);
                case "view" -> convertViewDefinition(rs);
                case "function" -> convertFunctionDefinition(rs);
                case "trigger" -> convertTriggerDefinition(rs);
                case "procedure" -> convertProcedureDefinition(rs);
                case "foreign_key" -> convertForeignKeyDefinition(rs);
                case "constraint" -> convertConstraintDefinition(rs);
                default -> rs.getString("definition");
            };
        } catch (SQLException e) {
            throw new TranslatorException(ErrorCode.SQL_TRANSLATION_FAILED.getCode(),
                    "Error converting definition for object type " + objectType, e);
        }
    }

    /**
     * translate view definition from result set
     * 
     * @param rs ResultSet Object
     * @return View Definition
     */
    private String convertViewDefinition(ResultSet rs) throws SQLException, TranslatorException {
        try {
            String name = rs.getString("name");
            String definition = getColumnClobValue(rs, "definition");
            return TRANSLATOR.translateView(name,definition).get();
        } catch (SQLException e) {
            throw new TranslatorException(ErrorCode.SQL_TRANSLATION_FAILED.getCode(),
                    "Error converting definition for object type view", e);
        }
    }

    /**
     * translate foreign key definition from result set
     * 
     * @param rs ResultSet Object
     * @return Foreign Key Definition
     */
    private String convertForeignKeyDefinition(ResultSet rs) throws TranslatorException {
        try {
            return rs.getString("definition");
        } catch (SQLException e) {
            throw new TranslatorException(ErrorCode.SQL_TRANSLATION_FAILED.getCode(),
                    "Error converting definition for object type foreign_key", e);
        }
    }

    /**
     * translate constraint definition from result set
     * 
     * @param rs ResultSet Object
     * @return Constraint Definition
     */
    private String convertConstraintDefinition(ResultSet rs) throws TranslatorException {
        return getColumnLongValue(rs, "definition");
    }

    /**
     * translate sequence definition from result set
     * 
     * @param rs ResultSet Object
     * @return Sequence Definition
     */
    private String convertSequenceDefinition(ResultSet rs) throws SQLException, TranslatorException {
        String sequenceName = rs.getString("name");
        if (sequenceName == null) {
            LOGGER.warn("Sequence name is null, skipping");
            return null;
        }

        java.math.BigDecimal increment = getBigDecimalOrDefault(rs, "increment_by", java.math.BigDecimal.ONE);
        java.math.BigDecimal lastNumber = getBigDecimalOrDefault(rs, "last_number", java.math.BigDecimal.ONE);
        java.math.BigDecimal minValue = rs.getBigDecimal("min_value");
        java.math.BigDecimal maxValue = rs.getBigDecimal("max_value");

        int cacheSize = rs.getInt("cache_size");
        cacheSize = cacheSize == 0 ? 20 : cacheSize;

        boolean isCycling = "Y".equals(rs.getString("cycle_flag"));
        boolean isOrdered = "Y".equals(rs.getString("order_flag"));

        StringBuilder sequenceDdl = new StringBuilder();
        sequenceDdl.append("CREATE SEQUENCE ").append(sequenceName);
        sequenceDdl.append(" INCREMENT BY ").append(increment);
        sequenceDdl.append(" START WITH ").append(lastNumber);

        appendMinValue(sequenceDdl, minValue);
        appendMaxValue(sequenceDdl, maxValue);

        sequenceDdl.append(" ").append(isCycling ? "CYCLE" : "NOCYCLE");
        sequenceDdl.append(" CACHE ").append(cacheSize);
        sequenceDdl.append(" ").append(isOrdered ? "ORDER" : "NOORDER");

        return sequenceDdl.toString();
    }

    private java.math.BigDecimal getBigDecimalOrDefault(ResultSet rs, String columnName,
            java.math.BigDecimal defaultValue) throws SQLException {
        java.math.BigDecimal value = rs.getBigDecimal(columnName);
        return value != null ? value : defaultValue;
    }

    private void appendMinValue(StringBuilder builder, java.math.BigDecimal minValue) {
        if (minValue != null) {
            builder.append(" MINVALUE ").append(minValue);
        }
    }

    private void appendMaxValue(StringBuilder builder, java.math.BigDecimal maxValue) {
        java.math.BigDecimal maxAllowedValue = new java.math.BigDecimal("9223372036854775807"); // 2^63-1
        if (maxValue != null && maxValue.compareTo(maxAllowedValue) <= 0) {
            builder.append(" MAXVALUE ").append(maxValue);
        } else {
            builder.append(" NOMAXVALUE");
        }
    }

    /**
     * translate function definition from result set, remove
     * editionable/noneditionable keywords
     * 
     * @param rs ResultSet Object
     * @return Function Definition without EDITIONABLE/NONEDITIONABLE Keywords
     */
    private String convertFunctionDefinition(ResultSet rs) throws TranslatorException {
        try {
            return TRANSLATOR.translateFunction(getColumnClobValue(rs, "definition")).get();
        } catch (SQLException e) {
            throw new TranslatorException(ErrorCode.SQL_TRANSLATION_FAILED.getCode(),
                    "Failed to get column definition as character stream: " + e.getMessage(), e);
        }
    }

    /**
     * translate trigger definition from result set, remove
     * editionable/noneditionable keywords
     * 
     * @param rs ResultSet Object
     * @return Trigger Definition without EDITIONABLE/NONEDITIONABLE Keywords
     */
    private String convertTriggerDefinition(ResultSet rs) throws TranslatorException {
        try {
            return TRANSLATOR.translateTrigger(getColumnClobValue(rs, "definition")).get();
        } catch (SQLException e) {
            throw new TranslatorException(ErrorCode.SQL_TRANSLATION_FAILED.getCode(),
                    "Failed to get column definition: " + e.getMessage(), e);
        }
    }

    /**
     * translate procedure definition from result set, remove
     * editionable/noneditionable keywords
     * 
     * @param rs ResultSet Object
     * @return Procedure Definition without EDITIONABLE/NONEDITIONABLE Keywords
     */
    private String convertProcedureDefinition(ResultSet rs) throws TranslatorException {
        try {
            return TRANSLATOR.translateProcedure(getColumnClobValue(rs, "definition")).get();
        } catch (SQLException e) {
            throw new TranslatorException(ErrorCode.SQL_TRANSLATION_FAILED.getCode(),
                    "Failed to get column definition: " + e.getMessage(), e);
        }
    }

    /**
     * read long value from result set
     * 
     * @param rs         ResultSet Object
     * @param columnName Column Name
     * @return LONG Value
     */
    private synchronized String getColumnLongValue(ResultSet rs, String columnName) {
        // First try getString() for small values
        try {
            String value = rs.getString(columnName);
            if (value != null) {
                return value;
            }
        } catch (SQLException e) {
            LOGGER.debug("Failed to get column {} as string: {}", columnName, e.getMessage());
        }

        // Then try getCharacterStream()
        try (Reader reader = rs.getCharacterStream(columnName)) {
            return readStreamAsString(reader);
        } catch (Exception e) {
            LOGGER.debug("Failed to get column {} as character stream: {}", columnName, e.getMessage());
        }

        // If all methods fail, return null
        LOGGER.warn("All methods failed to get column {}", columnName);
        return null;
    }

    /**
     * read clob value from result set
     * 
     * @param rs         ResultSet Object
     * @param columnName Column Name
     * @return CLOB Value
     * @throws SQLException read clob value failed
     */
    private synchronized String getColumnClobValue(ResultSet rs, String columnName) throws SQLException {
        // First try getString() for small CLOBs
        try {
            String value = rs.getString(columnName);
            if (value != null) {
                return value;
            }
        } catch (SQLException e) {
            LOGGER.debug("Failed to get column {} as string: {}", columnName, e.getMessage());
        }

        // Then try getClob()
        java.sql.Clob clob = rs.getClob(columnName);
        if (clob != null) {
            try (Reader reader = clob.getCharacterStream()) {
                return readStreamAsString(reader);
            } catch (IOException e) {
                throw new SQLException("Failed to read CLOB value: " + e.getMessage(), e);
            } finally {
                try {
                    clob.free();
                } catch (SQLException e) {
                    LOGGER.warn("Error freeing CLOB {}: {}", columnName, e.getMessage());
                }
            }
        }
        return null;
    }

    private String readStreamAsString(Reader reader) throws IOException {
        if (reader == null) {
            return "";
        }
        try (reader) {
            StringBuilder sb = new StringBuilder();
            char[] buffer = new char[4096];
            int charsRead;
            while ((charsRead = reader.read(buffer)) != -1) {
                sb.append(buffer, 0, charsRead);
            }
            return sb.toString();
        }
    }

    @Override
    protected String getQueryIndexSql(String schema) {
        return OracleSqlConstants.QUERY_INDEX_SQL;
    }

    @Override
    protected TableIndex getTableIndex(Connection conn, ResultSet rs) throws SQLException {
        String indexExpression = null;
        boolean hasExpression = rs.getBoolean("has_expression");
        if (hasExpression) {
            indexExpression = getColumnLongValue(rs, "index_expression");
        }
        TableIndex tableIndex = new TableIndex(rs);
        if (hasExpression && indexExpression != null) {
            tableIndex.setIndexprs(indexExpression);
            LOGGER.info("expression index: {}", tableIndex.toString());
        }
        List<String> indexCols = new ArrayList<>();
        String queryIndexCol = String.format(OracleSqlConstants.QUERY_INDEX_COL_SQL, tableIndex.getTableName(),
                tableIndex.getIndexName());
        try (Statement stmt = conn.createStatement();
                ResultSet colRs = stmt.executeQuery(queryIndexCol)) {
            while (colRs.next()) {
                indexCols.add(DatabaseUtils.formatObjName(colRs.getString("column_name")));
            }
            tableIndex.setColumnName(String.join(CommonConstants.DELIMITER, indexCols));
        }
        return tableIndex;
    }

    @Override
    protected void confirmUniqueConstraint(Connection conn, String schema, TableIndex tableIndex) throws SQLException {

    }

    @Override
    protected String getQueryPkSql() {
        return OracleSqlConstants.QUERY_PK_SQL;
    }

    @Override
    protected String getQueryFkSql(String schema) {
        return String.format(OracleSqlConstants.QUERY_FK_SQL, schema.toUpperCase(Locale.ROOT));
    }

    @Override
    public void createSourceLogicalReplicationSlot(Connection conn) {
    }

    @Override
    public void dropSourceLogicalReplicationSlot(Connection conn) {
    }

    @Override
    protected void setReplicaIdentity(Table table) {
    }

    @Override
    protected void initPublication(Connection conn, List<String> migraTableNames) {
    }

    /**
     * set char used for columns
     * char_used definition: varchar2(10 byte) ,varchar2(10 char)
     * 
     * @param conn      connection
     * @param schema    schema
     * @param tableName table name
     * @param columns   columns
     */
    protected void setCharUsedForColumns(Connection conn, String schema, String tableName, List<Column> columns) {
        if (columns == null || columns.isEmpty()) {
            return;
        }
        Map<String, Column> columnMap = columns.stream()
                .collect(Collectors.toMap(col -> col.getName().toUpperCase(Locale.ROOT), col -> (col), (a, b) -> a));
        try (PreparedStatement pstmt = conn.prepareStatement(OracleSqlConstants.QUERY_COLUMN_CHAR_USED_SQL)) {
            pstmt.setString(1, tableName.toUpperCase(Locale.ROOT));
            try (ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    String columnName = rs.getString("column_name");
                    String charUsed = rs.getString("char_used");
                    if (charUsed == null || columnName == null) {
                        continue;
                    }
                    Column column = columnMap.get(columnName.toUpperCase(Locale.ROOT));
                    if (column != null) {
                        column.setCharUsed(getCharUsedName(charUsed));
                    }
                }
            }
        } catch (SQLException e) {
            LOGGER.warn("Failed to read CHAR_USED for {}.{}: {}", schema, tableName, e.getMessage());
        }
    }

    /**
     * set comment for columns
     * 
     * @param conn      connection
     * @param schema    schema
     * @param tableName table name
     * @param columns   columns
     */
    protected void setCommentForColumns(Connection conn, String schema, String tableName, List<Column> columns) {
        if (columns == null || columns.isEmpty()) {
            return;
        }
        Map<String, Column> columnMap = columns.stream()
                .collect(Collectors.toMap(col -> col.getName().toUpperCase(Locale.ROOT), col -> (col), (a, b) -> a));
        try (PreparedStatement pstmt = conn.prepareStatement(OracleSqlConstants.QUERY_COLUMN_COMMENT_SQL)) {
            pstmt.setString(1, tableName.toUpperCase(Locale.ROOT));
            try (ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    String columnName = rs.getString("column_name");
                    String comment = rs.getString("comments");
                    if (comment == null || columnName == null) {
                        continue;
                    }
                    Column column = columnMap.get(columnName.toUpperCase(Locale.ROOT));
                    if (column != null) {
                        column.setComment(comment);
                    }
                }
            }
        } catch (SQLException e) {
            LOGGER.warn("Failed to read COMMENT for {}.{}: {}", schema, tableName, e.getMessage());
        }
    }

    /**
     * get char used name from type
     *
     * @param type char used type
     * @return char used name
     */
    private static String getCharUsedName(String type) {
        return type != null ? CHAR_USED_MAP.get(type.toUpperCase(Locale.ROOT)) : null;
    }
}