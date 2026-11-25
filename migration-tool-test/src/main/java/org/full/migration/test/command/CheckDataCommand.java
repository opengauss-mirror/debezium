/*
 * Copyright 2026 Huawei Technologies Co., Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.full.migration.test.command;

import org.apache.commons.cli.CommandLine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.full.migration.test.cleanup.DatabaseCleanupService;
import org.full.migration.test.config.ConfigManager;

import java.sql.Connection;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * CheckDataCommand
 * Command to check data generation status
 * This command checks the number of rows in each test table
 */
public class CheckDataCommand implements TestCommand {
    private static final Logger LOGGER = LoggerFactory.getLogger(CheckDataCommand.class);

    @Override
    public void execute(CommandLine cmd) throws Exception {
        String configPath = cmd.getOptionValue("config");
        LOGGER.info("Checking data generation status with config: {}", configPath);

        ConfigManager configManager = new ConfigManager(configPath);
        DatabaseCleanupService cleanupService = new DatabaseCleanupService(configManager);

        Connection sourceConn = null;
        try {
            sourceConn = cleanupService.getSourceConnection();
            checkTableData(sourceConn);
        } finally {
            if (sourceConn != null) {
                try { sourceConn.close(); } catch (Exception e) { LOGGER.warn("Error closing connection: " + e.getMessage()); }
            }
        }
    }

    /**
     * Check data in all test tables
     * @param conn Database connection
     * @throws SQLException if SQL execution fails
     */
    private void checkTableData(Connection conn) throws SQLException {
        LOGGER.info("Checking data in test tables...");
        printTableData(conn);
        checkTablespaceUsage(conn);
        LOGGER.info("Data check completed");
    }

    /**
     * print table data information
     * @param conn Database connection
     * @throws SQLException if SQL execution fails
     */
    private void printTableData(Connection conn) throws SQLException {
        String sql = """
                    SELECT t.table_name, t.tablespace_name, t.num_rows, t.avg_row_len, t.last_analyzed,
                        ROUND(NVL(t.num_rows * t.avg_row_len, 0) / 1024 / 1024 / 1024, 2) AS estimated_gb
                    FROM user_tables t 
                    WHERE t.table_name NOT LIKE 'BIN$%' 
                    AND t.table_name NOT LIKE 'DR$%' 
                    ORDER BY t.table_name 
                """;

        try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(sql)) {

            System.out.println("TABLE_NAME\t\t\tTABLESPACE_NAME\t\tNUM_ROWS\tAVG_ROW_LEN\tLAST_ANALYZED\t\tESTIMATED_GB");
            System.out.println("----------------------------------------------------------------------------------------------------");

            double totalSize = 0;
            long totalRows = 0;
            while (rs.next()) {
                String tableName = rs.getString("table_name");
                String tablespaceName = rs.getString("tablespace_name");
                int numRows = rs.getInt("num_rows");
                int avgRowLen = rs.getInt("avg_row_len");
                java.sql.Timestamp lastAnalyzed = rs.getTimestamp("last_analyzed");
                double gbUsed = rs.getDouble("estimated_gb");
                totalSize += gbUsed;
                totalRows += numRows;

                System.out.printf("%-30s\t%-15s\t%-10d\t%-10d\t%-20s\t%.2f%n", 
                    tableName, tablespaceName, numRows, avgRowLen,
                    lastAnalyzed != null ? lastAnalyzed.toString() : "null", gbUsed);
            }
            System.out.println("----------------------------------------------------------------------------------------------------");
            System.out.printf("TOTAL\t\t\t\t\t\t%-10d\t\t\t\t\t%.2f GB%n", totalRows, totalSize);
            System.out.println("----------------------------------------------------------------------------------------------------");
        }
    }
        

    /**
     * Check USERS tablespace usage
     * @param conn Database connection
     * @throws SQLException if SQL execution fails
     */
    private void checkTablespaceUsage(Connection conn) throws SQLException {
        LOGGER.info("Checking USERS tablespace usage...");

        String tablespaceSql = """
                SELECT tablespace_name, ROUND(SUM(bytes)/1024/1024/1024, 2) GB_used,
                       ROUND(SUM(DECODE(autoextensible, 'YES', maxbytes, bytes))/1024/1024/1024, 2) GB_max,
                       ROUND(SUM(bytes)/SUM(DECODE(autoextensible, 'YES', maxbytes, bytes))*100, 2) pct_used
                FROM dba_data_files WHERE tablespace_name='USERS' GROUP BY tablespace_name
                """;
        try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(tablespaceSql)) {
            System.out.println("\nUSERS TABLESPACE USAGE:");
            System.out.println("----------------------------------------------------------------------------------------------------");
            System.out.println("TABLESPACE_NAME\tGB_USED\tGB_MAX\tPCT_USED");
            System.out.println("----------------------------------------------------------------------------------------------------");

            while (rs.next()) {
                String tablespaceName = rs.getString("tablespace_name");
                double gbUsed = rs.getDouble("GB_used");
                double gbMax = rs.getDouble("GB_max");
                double pctUsed = rs.getDouble("pct_used");

                System.out.printf("%-20s\t%.2f\t%.2f\t%.2f%%%n", 
                    tablespaceName, gbUsed, gbMax, pctUsed);
            }
            System.out.println("----------------------------------------------------------------------------------------------------");
        } catch (SQLException e) {
            LOGGER.warn("Error checking tablespace usage: " + e.getMessage());
            System.out.println("\nUSERS TABLESPACE USAGE:");
            System.out.println("Error: Could not check tablespace usage - " + e.getMessage());
            System.out.println("Maybe you need to grant SELECT permission on dba_data_files to the user. ");
            System.out.println("For example: GRANT SELECT ON dba_data_files TO current_user;");
            System.out.println("----------------------------------------------------------------------------------------------------");
        }
    }
}