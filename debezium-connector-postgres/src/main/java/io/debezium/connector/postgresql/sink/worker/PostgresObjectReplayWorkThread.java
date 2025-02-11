/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package io.debezium.connector.postgresql.sink.worker;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.ArrayList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.postgresql.migration.PostgresSqlConstant;
import io.debezium.connector.postgresql.process.ProgressInfo;
import io.debezium.connector.postgresql.process.ProgressStatus;
import io.debezium.connector.postgresql.sink.object.ConnectionInfo;
import io.debezium.connector.postgresql.sink.record.SinkObjectRecord;
import io.debezium.connector.postgresql.sink.utils.ConnectionUtil;
import io.debezium.migration.ObjectEnum;
import io.debezium.sink.worker.ReplayWorkThread;

/**
 * PostgresObjectReplayWorkThread class
 *
 * @author jianghongbo
 * @since 2025/2/6
 */
public class PostgresObjectReplayWorkThread extends ReplayWorkThread {
    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresObjectReplayWorkThread.class);
    private static final String DELIMITER = System.lineSeparator();
    private static final String CREATE_PREFIX = "CREATE OR REPLACE %s";
    private static final String VIEW = ObjectEnum.VIEW.code();
    private static final String FUNCTION = ObjectEnum.FUNCTION.code();
    private static final String TRIGGER = ObjectEnum.TRIGGER.code();
    private static final String PROCEDURE = ObjectEnum.PROCEDURE.code();

    private final BlockingQueue<SinkObjectRecord> sinkObjRecordQueue = new LinkedBlockingDeque<>();
    private final ConnectionInfo connectionInfo;
    private final boolean isCommitProcess;

    private Statement statement = null;
    private Connection connection;
    private boolean isRunning = false;
    private boolean shouldStop = false;
    private Map<ObjectEnum, List<ProgressInfo>> objectProgressMap;
    private Map<String, String> schemaMappingMap = new HashMap<>();

    /**
     * Constructor
     *
     * @param schemaMappingMap schemaMappingMap information
     * @param connectionInfo ConnectionInfo  the connection information
     * @param isCommitProcess boolean
     * @param objectProgressMap Map<ObjectEnum, List<ProgressInfo>>
     */
    public PostgresObjectReplayWorkThread(Map<String, String> schemaMappingMap,
                                          ConnectionInfo connectionInfo,
                                          boolean isCommitProcess,
                                          Map<ObjectEnum, List<ProgressInfo>> objectProgressMap) {
        super(1);
        this.schemaMappingMap = schemaMappingMap;
        this.connectionInfo = connectionInfo;
        this.isCommitProcess = isCommitProcess;
        this.objectProgressMap = objectProgressMap;
    }

    @Override
    public void run() {
        SinkObjectRecord sinkObjectRecord = null;
        connection = ConnectionUtil.createConnection(connectionInfo);
        statement = createStatement(connection, 1);
        while (true) {
            String objName = null;
            ObjectEnum objType = null;
            try {
                if (shouldStop && sinkObjRecordQueue.isEmpty()) {
                    statement.close();
                    connection.close();
                    LOGGER.info("object replay thread exiting");
                    break;
                }
                sinkObjectRecord = sinkObjRecordQueue.take();
                String ddl = sinkObjectRecord.getDdl();
                String sourceSchema = sinkObjectRecord.getSchema();
                String schema = schemaMappingMap.getOrDefault(sourceSchema, sourceSchema);
                objType = sinkObjectRecord.getObjType();
                objName = sinkObjectRecord.getObjName();
                statement.execute(String.format(Locale.ROOT, PostgresSqlConstant.SWITCHSCHEMA, schema));
                String finalDdl = getFinalDdl(ddl, objType.code(), sourceSchema, schema);
                LOGGER.info("replay ddl: {}", finalDdl);
                statement.execute(finalDdl);
                computeProcess(objName, ProgressStatus.MIGRATED_COMPLETE, objType, "");
            } catch (InterruptedException | SQLException e) {
                computeProcess(objName, ProgressStatus.MIGRATED_FAILURE, objType, e.getMessage());
                LOGGER.error("replay object occurred Exception", e);
            }
        }
    }

    private String getFinalDdl(String originDdl, String objType, String sourceSchema, String sinkSchema) {
        if (TRIGGER.equals(objType)) {
            return originDdl.replace(sourceSchema + ".", sinkSchema + ".");
        }

        String trimOriginDdl = originDdl.trim();
        String createObjPrefix = String.format(CREATE_PREFIX, objType);
        if (FUNCTION.equals(objType)) {
            if (trimOriginDdl.startsWith(String.format(CREATE_PREFIX, FUNCTION))) {
                createObjPrefix = String.format(CREATE_PREFIX, FUNCTION);
            } else if (trimOriginDdl.startsWith(String.format(CREATE_PREFIX, PROCEDURE))) {
                createObjPrefix = String.format(CREATE_PREFIX, PROCEDURE);
            }
        }
        String subDdl = trimOriginDdl.substring(createObjPrefix.length());
        int dotIdx = subDdl.indexOf(".");
        subDdl = subDdl.substring(dotIdx + 1);
        String finalDdl = (createObjPrefix + " " + subDdl)
                .replace(sourceSchema + ".", sinkSchema + ".");
        return finalDdl;
    }

    private void computeProcess(String objectName, ProgressStatus status, ObjectEnum objType, String error) {
        if (objectName != null && objType != null) {
            ProgressInfo progressInfo = new ProgressInfo();
            progressInfo.setName(objectName);
            progressInfo.setPercent(1);
            progressInfo.setRecord(1);
            progressInfo.setError(error);
            progressInfo.setStatus(status.getCode());
            objectProgressMap.computeIfAbsent(objType, k -> new ArrayList<>());
            List<ProgressInfo> objectInfo = objectProgressMap.get(objType);
            objectInfo.add(progressInfo);
        }
    }

    /**
     * add object record to queue
     *
     * @param sinkObjectRecord SinkObjectRecord
     */
    public void addData(SinkObjectRecord sinkObjectRecord) {
        sinkObjRecordQueue.add(sinkObjectRecord);
    }

    /**
     * Get work thread queue length
     *
     * @return int the queue length in work thread
     */
    public int getQueueLength() {
        return sinkObjRecordQueue.size();
    }

    /**
     * Gets connection.
     *
     * @return the value of connection
     */
    public Connection getConnection() {
        return connection;
    }

    /**
     * Sets the connection.
     *
     * @param connection the connection
     */
    public void setConnection(Connection connection) {
        this.connection = connection;
    }

    public boolean isRunning() {
        return isRunning;
    }

    public void setRunning(boolean isRunning) {
        this.isRunning = isRunning;
    }

    public boolean isShouldStop() {
        return shouldStop;
    }

    public void setShouldStop(boolean shouldStop) {
        this.shouldStop = shouldStop;
    }
}
