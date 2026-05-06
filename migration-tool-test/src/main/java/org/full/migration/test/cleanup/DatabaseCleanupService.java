/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */

package org.full.migration.test.cleanup;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.full.migration.test.config.ConfigManager;
import org.full.migration.test.sql.DatabaseSqlProvider;
import org.full.migration.test.sql.SqlProviderFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Handles database cleanup operations for oracle2ograc migration testing
 * This service provides functionality to clean up database objects (tables, indexes,
 * triggers, procedures, functions, views, sequences) from both source and target databases
 */
public class DatabaseCleanupService implements EnvironmentCleanupService {
    private static final Logger LOGGER = LoggerFactory.getLogger(DatabaseCleanupService.class);
    private final ConfigManager configManager;
    private Connection sourceConn;
    private Connection targetConn;

    static {
        try {
            Class.forName("oracle.jdbc.OracleDriver");
            LOGGER.info("Oracle driver loaded successfully");
        } catch (ClassNotFoundException e) {
            LOGGER.error("Oracle driver not found: {}", e.getMessage());
        }
    }

    static {
        try {
            Class.forName("org.opengauss.Driver");
            LOGGER.info("oGRAC driver loaded successfully");
        } catch (ClassNotFoundException e) {
            LOGGER.error("oGRAC driver not found: {}", e.getMessage());
        }
    }

    /**
     * Constructor
     * @param configManager Configuration manager instance
     */
    public DatabaseCleanupService(ConfigManager configManager) {
        this.configManager = configManager;
    }

    /**
     * Get database connection for the specified type
     * @param type Connection type (source or target)
     * @return Database connection
     * @throws SQLException if connection fails
     */
    private Connection getConnection(String type) throws SQLException {
        String url = configManager.getConfigValue(type, "url", "");
        String username = configManager.getConfigValue(type, "username", "");
        String password = configManager.getConfigValue(type, "password", "");
        
        java.util.Properties props = new java.util.Properties();
        props.setProperty("user", username);
        props.setProperty("password", password);
        
        if (type.equals("source")) {
            if (sourceConn == null || sourceConn.isClosed()) {
                if (url.contains("oracle")) {
                    props.setProperty("oracle.jdbc.defaultNChar", "true"); 
                }
                sourceConn = DriverManager.getConnection(url, props);
            }
            return sourceConn;
        } else {
            if (targetConn == null || targetConn.isClosed()) {
                targetConn = DriverManager.getConnection(url, props);
            }
            return targetConn;
        }
    }

    @Override
    public Connection getSourceConnection() throws SQLException {
        return getConnection("source");
    }

    @Override
    public Connection getTargetConnection() throws SQLException {
        return getConnection("target");
    }

    /**
     * Get schema from configuration
     * @param type Schema type (source or target)
     * @return Schema name
     */
    public String getSchema(String type) {
        return configManager.getConfigValue(type, "username", "");
    }

    /**
     * Get source schema from configuration
     * @return Source schema
     */
    public String getSourceSchema() {
        return getSchema("source");
    }

    /**
     * Get target schema from configuration
     * @return Target schema
     */
    public String getTargetSchema() {
        return getSchema("target");
    }

    @Override
    public String getTestScenarioPath() {
        return configManager.getTestScenarioPath();
    }

    @Override
    public String getTestLog() {
        return configManager.getTestLog();
    }

    @Override
    public String getOwner() {
        return configManager.getOwner();
    }

    @Override
    public void cleanup() throws Exception {
        LOGGER.info("Starting environment cleanup...");
        ExecutorService executor = Executors.newFixedThreadPool(2);
        executor.submit(() -> {
            try {
                cleanupSourceEnvironment();
            } catch (Exception e) {
                LOGGER.error("Source environment cleanup failed: " + e.getMessage(), e);
            }
        });
        executor.submit(() -> {
            try {
                cleanupTargetEnvironment();
            } catch (Exception e) {
                LOGGER.error("Target environment cleanup failed: " + e.getMessage(), e);
            }
        });
        executor.shutdown();
        executor.awaitTermination(5, TimeUnit.MINUTES);
        LOGGER.info("Environment cleanup completed!");
    }

    /**
     * Clean up source environment
     * @throws Exception if cleanup fails
     */
    private void cleanupSourceEnvironment() throws Exception {
        LOGGER.info("Cleaning up source environment...");
        Connection conn = getSourceConnection();
        cleanupDatabase(conn, "source");
    }

    /**
     * Clean up target environment
     * @throws Exception if cleanup fails
     */
    private void cleanupTargetEnvironment() throws Exception {
        LOGGER.info("Cleaning up target environment...");
        Connection conn = getTargetConnection();
        cleanupDatabase(conn, "target");
    }

    /**
     * Clean up database objects
     * Cleans up triggers, procedures, functions, views, indexes, tables, and sequences
     * @param conn Database connection
     * @param type Database type (source or target)
     * @throws Exception if cleanup fails
     */
    private void cleanupDatabase(Connection conn, String type) throws Exception {
        String dbType = configManager.getDatabaseType(type);
        DatabaseSqlProvider sqlProvider = SqlProviderFactory.getSqlProvider(dbType);
        LOGGER.info("Cleaning up {} database ({} type)...", type, dbType);
        sqlProvider.setSchema("source".equals(type) ? getSourceSchema() : getTargetSchema());
        cleanupTriggers(conn, type, sqlProvider);
        cleanupProcedures(conn, type, sqlProvider);
        cleanupFunctions(conn, type, sqlProvider);
        cleanupViews(conn, type, sqlProvider);
        cleanupIndexes(conn, type, sqlProvider);
        cleanupTables(conn, type, sqlProvider);
        cleanupSequences(conn, type, sqlProvider);
    }

    /**
     * Clean up triggers
     * @param conn Database connection
     * @param type Database type (source or target)
     * @param sqlProvider SQL provider for database-specific SQL statements
     * @throws Exception if cleanup fails
     */
    private void cleanupTriggers(Connection conn, String type, DatabaseSqlProvider sqlProvider) throws Exception {
        LOGGER.info("Cleaning up {} triggers...", type);
        List<String> triggers = getObjects(conn, sqlProvider.getTriggersQuery());
        LOGGER.info("Found {} triggers: {}", triggers.size(), triggers);
        for (String trigger : triggers) {
            String dropSql = sqlProvider.getDropTriggerSql(trigger);
            executeSql(conn, dropSql);
        }
    }

    /**
     * Clean up procedures
     * @param conn Database connection
     * @param type Database type (source or target)
     * @param sqlProvider SQL provider for database-specific SQL statements
     * @throws Exception if cleanup fails
     */
    private void cleanupProcedures(Connection conn, String type, DatabaseSqlProvider sqlProvider) throws Exception {
        LOGGER.info("Cleaning up {} procedures...", type);
        List<String> procedures = getObjects(conn, sqlProvider.getProceduresQuery());
        LOGGER.info("Found {} procedures: {}", procedures.size(), procedures);
        for (String procedure : procedures) {
            String dropSql = sqlProvider.getDropProcedureSql(procedure);
            executeSql(conn, dropSql);
        }
    }

    /**
     * Clean up functions
     * @param conn Database connection
     * @param type Database type (source or target)
     * @param sqlProvider SQL provider for database-specific SQL statements
     * @throws Exception if cleanup fails
     */
    private void cleanupFunctions(Connection conn, String type, DatabaseSqlProvider sqlProvider) throws Exception {
        LOGGER.info("Cleaning up {} functions...", type);
        List<String> functions = getObjects(conn, sqlProvider.getFunctionsQuery());
        LOGGER.info("Found {} functions: {}", functions.size(), functions);
        for (String function : functions) {
            String dropSql = sqlProvider.getDropFunctionSql(function);
            executeSql(conn, dropSql);
        }
    }

    /**
     * Clean up views
     * @param conn Database connection
     * @param type Database type (source or target)
     * @param sqlProvider SQL provider for database-specific SQL statements
     * @throws Exception if cleanup fails
     */
    private void cleanupViews(Connection conn, String type, DatabaseSqlProvider sqlProvider) throws Exception {
        LOGGER.info("Cleaning up {} views...", type);
        List<String> views = getObjects(conn, sqlProvider.getViewsQuery());
        LOGGER.info("Found {} views: {}", views.size(), views);
        for (String view : views) {
            String dropSql = sqlProvider.getDropViewSql(view);
            executeSql(conn, dropSql);
        }
    }

    /**
     * Clean up indexes
     * @param conn Database connection
     * @param type Database type (source or target)
     * @param sqlProvider SQL provider for database-specific SQL statements
     * @throws Exception if cleanup fails
     */
    private void cleanupIndexes(Connection conn, String type, DatabaseSqlProvider sqlProvider) throws Exception {
        LOGGER.info("Cleaning up {} indexes...", type);
        List<String> indexes = getObjects(conn, sqlProvider.getIndexesQuery());
        LOGGER.info("Found {} indexes: {}", indexes.size(), indexes);
        for (String index : indexes) {
            String dropSql = sqlProvider.getDropIndexSql(index);
            executeSql(conn, dropSql);
        }
    }

    /**
     * Clean up tables
     * @param conn Database connection
     * @param type Database type (source or target)
     * @param sqlProvider SQL provider for database-specific SQL statements
     * @throws Exception if cleanup fails
     */
    private void cleanupTables(Connection conn, String type, DatabaseSqlProvider sqlProvider) throws Exception {
        LOGGER.info("Cleaning up {} tables...", type);
        List<String> tables = getObjects(conn, sqlProvider.getTablesQuery());
        LOGGER.info("Found {} tables: {}", tables.size(), tables);
        for (String table : tables) {
            String dropSql = sqlProvider.getDropTableSql(table);
            executeSql(conn, dropSql);
        }
    }

    /**
     * Clean up sequences
     * @param conn Database connection
     * @param type Database type (source or target)
     * @param sqlProvider SQL provider for database-specific SQL statements
     * @throws Exception if cleanup fails
     */
    private void cleanupSequences(Connection conn, String type, DatabaseSqlProvider sqlProvider) throws Exception {
        LOGGER.info("Cleaning up {} sequences...", type);
        List<String> sequences = getObjects(conn, sqlProvider.getSequencesQuery());
        LOGGER.info("Found {} sequences: {}", sequences.size(), sequences);
        for (String sequence : sequences) {
            String dropSql = sqlProvider.getDropSequenceSql(sequence);
            executeSql(conn, dropSql);
        }
    }

    /**
     * Get list of objects from database
     * @param conn Database connection
     * @param query SQL query to execute
     * @return List of object names
     * @throws Exception if query execution fails
     */
    private List<String> getObjects(Connection conn, String query) throws Exception {
        List<String> objects = new java.util.ArrayList<>();
        try (java.sql.Statement stmt = conn.createStatement();
                java.sql.ResultSet rs = stmt.executeQuery(query)) {
            while (rs.next()) {
                objects.add(rs.getString(1));
            }
        }
        return objects;
    }

    /**
     * Execute SQL statement
     * @param conn Database connection
     * @param sql SQL statement to execute
     */
    private void executeSql(Connection conn, String sql) {
        try (java.sql.Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
        } catch (SQLException e) {
            // Ignore ORA-02429 error (cannot drop index used for enforcement of unique/primary key)
            // These indexes will be automatically dropped when the table is dropped
            if (e.getErrorCode() == 2429) {
                LOGGER.debug("Ignoring ORA-02429 error: {}", e.getMessage());
            } else {
                LOGGER.warn("Error executing SQL: {} - {}", sql, e.getMessage());
            }
        } catch (Exception e) {
            LOGGER.warn("Error executing SQL: {} - {}", sql, e.getMessage());
        }
    }

    /**
     * Close database connections
     */
    public void closeConnections() {
        if (sourceConn != null) {
            try {
                sourceConn.close();
            } catch (Exception e) {
                LOGGER.warn("Error closing source connection: " + e.getMessage());
            }
        }
        if (targetConn != null) {
            try {
                targetConn.close();
            } catch (Exception e) {
                LOGGER.warn("Error closing target connection: " + e.getMessage());
            }
        }
    }
}