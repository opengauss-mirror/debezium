/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */

package org.full.migration.datax.config;

import org.full.migration.datax.model.*;
import org.full.migration.exception.DataXMigrationException;
import org.full.migration.exception.ErrorCode;
import org.full.migration.model.config.DatabaseConfig;
import org.full.migration.model.table.Table;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * GeneralDataXConfigStrategy
 * General DataX configuration strategy for all table types
 * This strategy dynamically adjusts configuration based on table properties
 *
 * @since 2025-04-18
 */
public class GeneralDataXConfigStrategy implements DataXConfigStrategy {
    private static final String STRATEGY_NAME = "general_datax_strategy";
    private static final String STRATEGY_DESCRIPTION = "General DataX configuration strategy for all table types, " +
            "dynamically adjusts configuration based on table properties";

    @Override
    public DataXConfig generateConfig(DataXConfigContext context) throws DataXMigrationException {
        DatabaseConfig sourceConfig = context.getSourceConfig();
        DatabaseConfig targetConfig = context.getTargetConfig();
        String schemaName = context.getSchemaName();
        String targetSchemaName = context.getTargetSchemaName();
        Table table = context.getTable();
        DataXCommonConfig commonConfig = context.getCommonConfig();

        DataXConfig config = new DataXConfig();
        Job job = config.getJob();
        Setting setting = job.getSetting();
        setting.getSpeed().setChannel(getChannelCount(table.getEstimatedRowCount()));
        setting.getErrorLimit().setRecord(commonConfig.getErrorRecordLimit());
        setting.getErrorLimit().setPercentage(commonConfig.getErrorPercentageLimit());

        Content content = new Content();
        configureReader(content.getReader(), sourceConfig, schemaName, table, commonConfig);
        configureWriter(content.getWriter(), targetConfig, targetSchemaName, table, commonConfig);
        job.addContent(content);
        return config;
    }

    @Override
    public boolean isApplicable(DataXConfigContext context) {
        // General strategy applies to all tables
        return true;
    }

    @Override
    public String getStrategyName() {
        return STRATEGY_NAME;
    }

    @Override
    public String getDescription() {
        return STRATEGY_DESCRIPTION;
    }

    @Override
    public String getJvmParameters(long rowCount) {
        if (rowCount <= 10000) {
            return "-Xms512m -Xmx512m";
        }
        if (rowCount <= 1000000) {
            return "-Xms1g -Xmx1g";
        }
        if (rowCount <= 10000000) {
            return "-Xms2g -Xmx2g";
        }
        return "-Xms4g -Xmx4g";
    }

    /**
     * Get the number of channels based on table size
     * 
     * @param rowCount Row count
     * @return Number of channels
     */
    protected int getChannelCount(long rowCount) {
        if (rowCount <= 10000) return 1; // Less than 10,000
        if (rowCount <= 100000) return 2; // 10,000 - 100,000
        if (rowCount <= 1000000) return 4; // 100,000 - 1,000,000
        return Math.min(8, Runtime.getRuntime().availableProcessors() / 2); // More than 1,000,000
    }

    /**
     * <pre>
     * Configure the reader part
     * if table has no pk or multiple primary keys, set split key to empty string, 
     * oracleReader will auto split by rowid.
     * else set split key to primary key column name
     * </pre>
     * @param reader       reader object
     * @param sourceConfig Source database configuration
     * @param schemaName   Schema name
     * @param table        Table object
     * @param commonConfig Common configuration
     */
    protected void configureReader(Reader reader, DatabaseConfig sourceConfig, String schemaName, 
                                   Table table, DataXCommonConfig commonConfig) throws DataXMigrationException {
        reader.setName(commonConfig.getReaderName());
        ReaderParameter readerParam = reader.getParameter();
        readerParam.setUsername(commonConfig.getReaderUsername());
        readerParam.setPassword(commonConfig.getReaderPassword());
        readerParam.setBatchSize(getReaderBatchSize(table.getEstimatedRowCount()));
        readerParam.setQueryTimeout(commonConfig.getReadTimeout() / 1000);
        String splitPk = getSplitPk(table);
        if (splitPk != null && !splitPk.isEmpty()) {
            readerParam.setSplitPk(splitPk);
        }
        ReaderConnection readerConn = new ReaderConnection();
        readerConn.setJdbcUrl(Collections.singletonList(commonConfig.getReaderJdbcUrl()));
        readerConn.setSchema(schemaName);
        readerConn.addTable(schemaName + "." + table.getTableName());
        readerParam.setColumn(buildDataXQueryColumn(table.getTbColumns()));
        readerParam.addConnection(readerConn);
    }

    /**
     * Get the split key
     * datax split key column name
     * if table has multiple primary keys, return empty string
     * else return primary key column name
     * @param table Table object
     * @return Split key column name
     */
    protected String getSplitPk(Table table) throws DataXMigrationException {
        if (!table.isHasPrimaryKey()) {
            return "";
        }
        if(table.getPkColumns() == null || table.getPkColumns().isEmpty()) {
            throw new DataXMigrationException(ErrorCode.DATAX_CONFIG_ERROR.getCode(),
                    "table "+ table.getTableName()+ " get primary key list has some error, list empty");
        }
        return table.getPkColumns().contains(",") ? "" : table.getPkColumns();
    }

    /**
     * Get the batch size based on table size
     * This is a helper method used by getReaderBatchSize, getWriterBatchSize, and getBatchInsertSize
     *
     * @param rowCount Table row count
     * @return Batch size
     */
    private int getBatchSizeByTableSize(long rowCount) {
        if (rowCount <= 10000) return 1000; // Less than 10,000
        if (rowCount <= 1000000) return 2000; // 10,000 - 1,000,000
        if (rowCount <= 10000000) return 4000; // 1,000,000 - 10,000,000
        return 4000; // More than 10,000,000
    }

    /**
     * Configure the writer part
     *
     * @param writer           writer object
     * @param targetConfig     Target database configuration
     * @param targetSchemaName Target schema name
     * @param table            Table object
     * @param commonConfig     Common configuration
     */
    protected void configureWriter(Writer writer, DatabaseConfig targetConfig, String targetSchemaName, 
                                   Table table, DataXCommonConfig commonConfig) {
        writer.setName(commonConfig.getWriterName());
        WriterParameter writerParam = writer.getParameter();
        writerParam.setUsername(commonConfig.getWriterUsername());
        writerParam.setPassword(commonConfig.getWriterPassword());
        writerParam.setBatchSize(getWriterBatchSize(table.getEstimatedRowCount()));
        writerParam.setBatchInsertSize(getBatchInsertSize(table.getEstimatedRowCount()));
        writerParam.setColumn(buildDataXWriterColumn(table.getTbColumns()));
        configurePreAndPostSql(writerParam, targetSchemaName, table.getTableName());
        WriterConnection writerConn = new WriterConnection();
        writerConn.setJdbcUrl(commonConfig.getWriterJdbcUrl());
        writerConn.addTable(table.getTableName());
        writerConn.setSchema(targetSchemaName);
        writerParam.addConnection(writerConn);
    }

    private List<String> buildDataXQueryColumn(String tbColumns) {
        return Arrays.stream(tbColumns.split(",")).map(String::trim).collect(Collectors.toList());
    }

    private List<String> buildDataXWriterColumn(String tbColumns) {
        return Arrays.stream(tbColumns.split(",")).map(col-> {
            if(col.contains("XMLSerialize")){
                // 提取实际列名称，处理 XMLSerialize 函数包裹的情况
                // XMLSerialize(CONTENT XML_COL AS CLOB) AS XML_COL -> XML_COL
                int startIdx = col.indexOf("CONTENT") + 7;
                int endIdx = col.indexOf("AS CLOB");
                String colName = col.substring(startIdx, endIdx).trim();
                return colName;
            }else{
                return col;
            }
        }).map(String::trim).collect(Collectors.toList());
    }
    /**
     * Configure the pre and post SQL
     * Used to control the creation, disabling, and enabling of table structures, indices, triggers, constraints, etc.
     * Pre-SQL: executed before data migration
     *          1. Disable constraints and triggers
     *              TRUNCATE TABLE targetSchemaName.tableName
     *              ALTER TABLE targetSchemaName.tableName DISABLE TRIGGER ALL
     *              ALTER TABLE targetSchemaName.tableName NOCHECK CONSTRAINT ALL;
     *          2. Create indices (example, actual should generate based on table structure)
     *              Here is just an example, actual project should generate indices based on table structure
     *              CREATE INDEX idx_tableName_col1 ON targetSchemaName.tableName (col1)
     *          3. Update statistics (optional)
     *              ANALYZE TABLE targetSchemaName.tableName
     * @param writerParam      writer parameter object
     * @param targetSchemaName Target schema name
     * @param tableName        Table name
     */
    protected void configurePreAndPostSql(WriterParameter writerParam, String targetSchemaName, String tableName) {
        writerParam.addPreSql("ALTER SESSION SET LOCK_WAIT_TIMEOUT = 0");
        writerParam.addPreSql("TRUNCATE TABLE " + targetSchemaName + "." + tableName);
    }

    /**
     * Get the reader batch size based on table size
     *
     * @param rowCount Table row count
     * @return Batch size
     */
    protected int getReaderBatchSize(long rowCount) {
        return getBatchSizeByTableSize(rowCount);
    }

    /**
     * Get the writer batch size based on table size
     *
     * @param rowCount Table row count
     * @return Batch size
     */
    protected int getWriterBatchSize(long rowCount) {
        return getBatchSizeByTableSize(rowCount);
    }

    /**
     * Get the batch insert size based on table size
     *
     * @param rowCount Table row count
     * @return Batch insert size
     */
    protected int getBatchInsertSize(long rowCount) {
        return getBatchSizeByTableSize(rowCount);
    }
}