/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.sink.replay;

import com.mysql.cj.util.StringUtils;
import io.debezium.connector.breakpoint.BreakPointRecord;
import io.debezium.connector.mysql.sink.object.ConnectionInfo;
import io.debezium.connector.mysql.sink.object.DdlOperation;
import io.debezium.connector.mysql.sink.object.SinkRecordObject;
import io.debezium.connector.mysql.sink.object.SourceField;
import io.debezium.connector.mysql.sink.task.MySqlSinkConnectorConfig;
import io.debezium.connector.mysql.sink.util.SqlTools;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;

/**
 * Description: Base task class
 *
 * @author douxin
 * @since 2023-06-26
 **/
public class ReplayTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(ReplayTask.class);
    private static final long INVALID_VALUE = -1L;

    /**
     * Sink queue, storing kafka records
     */
    protected BlockingQueue<SinkRecord> sinkQueue = new LinkedBlockingQueue<>();

    /**
     * schemaMappingMap
     */
    protected Map<String, String> schemaMappingMap = new HashMap<>();

    /**
     * openGaussConnection
     */
    protected ConnectionInfo openGaussConnection;

    /**
     * sql list
     */
    protected ArrayList<String> sqlList = new ArrayList<>();

    /**
     * primary table name
     */
    protected String primaryTable = "";

    /**
     * No schema data count
     */
    protected int noSchemaCount;

    /**
     * changed table name list
     */
    protected ArrayList<String> changedTableNameList = new ArrayList<>();

    /**
     * xlog location
     */
    protected String xlogLocation;

    /**
     * Sql tools
     */
    protected SqlTools sqlTools;

    /**
     * Break point record
     */
    protected BreakPointRecord breakPointRecord;

    /**
     * To delete offsets
     */
    protected List<Long> toDeleteOffsets;

    /**
     * Sink queue first offset
     */
    protected Long sinkQueueFirstOffset;

    /**
     * Adds queue map
     */
    protected Map<Long, String> addedQueueMap = new ConcurrentHashMap<>();
    private final HashMap<String, String> tableSnapshotHashmap = new HashMap<>();

    /**
     * Create work threads
     */
    public void createWorkThreads() {
    }

    /**
     * Batch write
     *
     * @param records Collection<SinkRecord> the records
     */
    public void batchWrite(Collection<SinkRecord> records) {
        sinkQueue.addAll(records);
    }

    /**
     * Do stop method
     */
    public void doStop() {
    }

    /**
     * update sourceField
     *
     * @param sourceField SourceField the sourceField
     */
    protected void updateTransaction(SourceField sourceField) {
    }

    /**
     * Write xlog location when stop the connector
     */
    protected void writeXlogResult() {
        String xlogResult = sqlTools.getXlogLocation();
        sqlTools.closeConnection();
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(xlogLocation))) {
            bw.write("xlog.location=" + xlogResult);
        }
        catch (IOException exp) {
            LOGGER.error("Fail to write xlog location {}", xlogResult);
        }
        LOGGER.info("Online migration from mysql to openGauss has gracefully stopped and current xlog"
                + "location in openGauss is {}", xlogResult);
    }

    /**
     * Initialize the xlog location
     *
     * @param xlogLocation String the xlog location
     */
    protected void initXlogLocation(String xlogLocation) {
        this.xlogLocation = xlogLocation;
    }

    /**
     * Update changed tables
     *
     * @param ddl String the ddl
     * @param newSchemaName String the new schema name
     * @param tableName String the table name
     */
    protected void updateChangedTables(String ddl, String newSchemaName, String tableName) {
        if (SqlTools.isCreateOrAlterTableStatement(ddl)) {
            changedTableNameList.add(newSchemaName + "." + tableName);
        }
    }

    /**
     * Is block
     *
     * @return true if is block
     */
    public boolean isBlock() {
        return false;
    }

    /**
     * Gets table snapshot
     */
    protected void getTableSnapshot() {
        Connection conn = openGaussConnection.createOpenGaussConnection();
        try {
            PreparedStatement ps = conn.prepareStatement("select v_schema_name, v_table_name, t_binlog_name,"
                    + " i_binlog_position from sch_chameleon.t_replica_tables;");
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                String schemaName = rs.getString("v_schema_name");
                String tableName = rs.getString("v_table_name");
                String binlogName = rs.getString("t_binlog_name");
                String binlogPosition = rs.getString("i_binlog_position");
                tableSnapshotHashmap.put(getSinkSchema(schemaName) + "." + tableName,
                        binlogName + ":" + binlogPosition);
            }
        }
        catch (SQLException exp) {
            LOGGER.warn("sch_chameleon.t_replica_tables does not exist.");
        }
    }

    /**
     * Gets sink database schema according to schema mapping
     *
     * @param sourceSchema String the source database schema name
     * @return String the sink database schema name
     */
    protected String getSinkSchema(String sourceSchema) {
        return schemaMappingMap.getOrDefault(sourceSchema, sourceSchema);
    }

    /**
     * Is skipped event
     *
     * @param sourceField SourceField the sourceField
     * @return boolean the isSkippedEvent
     */
    protected boolean isSkippedEvent(SourceField sourceField) {
        String schemaName = sourceField.getDatabase();
        String tableName = sourceField.getTable();
        String fullName = getSinkSchema(schemaName) + "." + tableName;
        if (tableSnapshotHashmap.containsKey(fullName)) {
            String binlogFile = sourceField.getFile();
            long fileIndex = Long.valueOf(binlogFile.split("\\.")[1]);
            long binlogPosition = sourceField.getPosition();
            String snapshotPoint = tableSnapshotHashmap.get(fullName);
            String snapshotBinlogFile = snapshotPoint.split(":")[0];
            String[] file = snapshotBinlogFile.split("\\.");
            String[] position = snapshotPoint.split(":");
            if (file.length >= 2 && position.length >= 2) {
                long snapshotFileIndex = Long.valueOf(file[1]);
                long snapshotBinlogPosition = Long.valueOf(position[1]);
                if (fileIndex < snapshotFileIndex
                        || (fileIndex == snapshotFileIndex && binlogPosition <= snapshotBinlogPosition)) {
                    String skipInfo = String.format("Table %s snapshot is %s, current position is %s, "
                            + "which is less than table snapshot, so skip the record.", fullName,
                            snapshotPoint, binlogFile + ":" + binlogPosition);
                    LOGGER.warn(skipInfo);
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Construct ddl
     *
     * @param sinkRecordObject SinkRecordObject the sinkRecordObject
     */
    protected void constructDdl(SinkRecordObject sinkRecordObject) {
        SourceField sourceField = sinkRecordObject.getSourceField();
        if (!(sinkRecordObject.getDataOperation() instanceof DdlOperation)) {
            return;
        }
        DdlOperation ddlOperation = (DdlOperation) sinkRecordObject.getDataOperation();
        String schemaName = sourceField.getDatabase();
        String tableName = sourceField.getTable();
        String newSchemaName = getSinkSchema(schemaName);
        String ddl = ddlOperation.getDdl();
        sqlList.add("set current_schema to " + newSchemaName + ";");
        if (StringUtils.isNullOrEmpty(tableName)) {
            sqlList.add(ddl);
        }
        else {
            ddl = rectifyForeignRelyDdl(ddl, sinkRecordObject);
            if ("".equals(ddl)) {
                sqlList.clear();
                return;
            }
            String modifiedDdl = null;
            if (ddl.toLowerCase(Locale.ROOT).startsWith("alter table")
                    && ddl.toLowerCase(Locale.ROOT).contains("rename to")
                    && !ddl.toLowerCase(Locale.ROOT).contains("`rename to")) {
                int preIndex = ddl.toLowerCase(Locale.ROOT).indexOf("table");
                int postIndex = ddl.toLowerCase(Locale.ROOT).indexOf("rename");
                String oldFullName = ddl.substring(preIndex + 6, postIndex).trim();
                if (oldFullName.split("\\.").length == 2) {
                    String oldName = oldFullName.split("\\.")[1];
                    modifiedDdl = ddl.replaceFirst(oldFullName, oldName);
                }
                else {
                    modifiedDdl = ddl;
                }
            }
            else if (ddl.toLowerCase(Locale.ROOT).startsWith("drop table")) {
                modifiedDdl = ddl.replaceFirst(SqlTools.addingBackQuote(schemaName) + ".", "");
            }
            else {
                modifiedDdl = ignoreSchemaName(ddl, schemaName, tableName);
            }
            sqlList.add(modifiedDdl);
            updateChangedTables(ddl, newSchemaName, tableName);
        }
        updateTransaction(sourceField);
    }

    /**
     * Rectify ddl for creating foreign keys
     *
     * @param ddl String the ddl sql used for creating foreign key,which possibly have created a foreign key's ddl
     *            when creating a table, or created a table first and then added a foreign key constraint's ddl.
     * @param sinkRecordObject SinkRecordObject the sinkRecordObject
     * @return String the rectified ddl, which means the source schema have been modified to sink schema
     */
    private String rectifyForeignRelyDdl(String ddl, SinkRecordObject sinkRecordObject) {
        String newDdl = ddl;
        if ((ddl.toLowerCase(Locale.ROOT).startsWith("alter table")
                || ddl.toLowerCase(Locale.ROOT).startsWith("create table"))
                && ddl.toLowerCase(Locale.ROOT).contains("foreign key")
                && ddl.toLowerCase(Locale.ROOT).contains("references")) {
            int index = ddl.indexOf("references");
            String ddlSuffix = ddl.substring(index + "references".length());
            if (ddlSuffix.split("\\.").length > 1) {
                String oldSchema = SqlTools.removeBackQuote(ddlSuffix.split("\\.")[0].trim());
                if (schemaMappingMap.containsKey(oldSchema)) {
                    ddlSuffix = ddlSuffix.replaceFirst(oldSchema, schemaMappingMap.get(oldSchema));
                    String[] targets = ddlSuffix.substring(0, ddlSuffix.lastIndexOf("(")).trim().split("\\.");
                    primaryTable = SqlTools.removeBackQuote(targets[0]) + "." + SqlTools.removeBackQuote(targets[1]);
                    ddlSuffix = ddlSuffix.replaceAll(targets[0],
                            SqlTools.addingQuote(SqlTools.removeBackQuote(targets[0])));
                    ddlSuffix = ddlSuffix.replaceAll(targets[1],
                            SqlTools.addingQuote(SqlTools.removeBackQuote(targets[1])));
                }
                else {
                    LOGGER.error("schema mapping of source schema {} is not initialized, "
                            + "this ddl will be ignored.", oldSchema);
                    noSchemaCount++;
                    return "";
                }
            }
            else {
                String tableNameInSql = ddlSuffix.trim().substring(0, ddlSuffix.trim()
                        .lastIndexOf("("));
                primaryTable = schemaMappingMap.get(sinkRecordObject.getSourceField().getDatabase())
                        + "." + SqlTools.removeBackQuote(tableNameInSql);
                ddlSuffix = ddlSuffix.replaceAll(tableNameInSql,
                        SqlTools.addingQuote(SqlTools.removeBackQuote(tableNameInSql)));
            }
            String ddlPrefix = ddl.substring(0, index);
            newDdl = ddlPrefix + " references " + ddlSuffix;
        }
        return newDdl;
    }

    private String ignoreSchemaName(String ddl, String schemaName, String tableName) {
        Set<String> schemaTableSet = new HashSet<>();
        schemaTableSet.add(schemaName + "." + tableName);
        schemaTableSet.add(SqlTools.addingBackQuote(schemaName) + "." + tableName);
        schemaTableSet.add(schemaName + "." + SqlTools.addingBackQuote(tableName));
        schemaTableSet.add(SqlTools.addingBackQuote(schemaName) + "." + SqlTools.addingBackQuote(tableName));
        for (String name : schemaTableSet) {
            if (ddl.contains(name)) {
                return ddl.replaceFirst(name, SqlTools.addingBackQuote(tableName));
            }
        }
        return ddl;
    }

    /**
     * Get the offset of already replayed record
     *
     * @return the continuous and maximum offset
     */
    public Long getReplayedOffset() {
        PriorityBlockingQueue<Long> replayedOffsets = breakPointRecord.getReplayedOffset();
        Long offset = replayedOffsets.peek();
        Long endOffset = INVALID_VALUE;
        if (replayedOffsets.isEmpty()) {
            return sinkQueueFirstOffset == null ? endOffset : sinkQueueFirstOffset;
        }
        boolean isContinuous = true;
        while (isContinuous && !replayedOffsets.isEmpty()) {
            Long num;
            try {
                num = replayedOffsets.take();
                if (num.equals(offset)) {
                    endOffset = num;
                }
                else {
                    replayedOffsets.offer(num);
                    isContinuous = false;
                }
                offset++;
            }
            catch (InterruptedException exp) {
                LOGGER.error("Interrupted exception occurred", exp);
            }
        }
        if (endOffset.equals(INVALID_VALUE)) {
            return endOffset;
        }
        replayedOffsets.offer(endOffset);
        Iterator<Map.Entry<Long, String>> iterator = addedQueueMap.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Long, String> entry = iterator.next();
            if (entry.getKey() < endOffset) {
                iterator.remove();
            }
        }
        toDeleteOffsets.add(endOffset);
        return endOffset + 1;
    }

    /**
     * Init breakpoint record properties
     *
     * @param config mysql sink connector config
     */
    protected void initRecordBreakpoint(MySqlSinkConnectorConfig config) {
        breakPointRecord = new BreakPointRecord(config);
        toDeleteOffsets = breakPointRecord.getToDeleteOffsets();
        breakPointRecord.start();
    }

    /**
     * Initialize the schema mappings
     *
     * @param schemaMappings String the schema
     */
    protected void initSchemaMappingMap(String schemaMappings) {
        String[] pairs = schemaMappings.split(";");
        for (String pair : pairs) {
            String[] schema = pair.split(":");
            if (schema.length == 2) {
                schemaMappingMap.put(schema[0].trim(), schema[1].trim());
            }
        }
    }
}
