/**
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.opengauss.sink.replay;

import io.debezium.connector.opengauss.sink.object.*;
import io.debezium.connector.opengauss.sink.utils.SqlTools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * Description: WorkThread class
 * @author wangzhengyuan
 * @date 2022/11/29
 */
public class WorkThread extends Thread {
    private static final Logger LOGGER = LoggerFactory.getLogger(WorkThread.class);
    private static final String INSERT = "c";
    private static final String UPDATE = "u";
    private static final String DELETE = "d";
    private ConnectionInfo connectionInfo;
    private SqlTools sqlTools;
    public int count;
    private BlockingQueue<SinkRecordObject> sinkRecordQueue = new LinkedBlockingDeque<>();
    private static Map<String, TableMetaData> oldTableMap = new HashMap<>();

    /**
     * Constructor
     *
     * @param connectionInfo ConnectionInfo the connectionInfo
     * @param sqlTools SqlTools the sqltools
     */
    public WorkThread(ConnectionInfo connectionInfo, SqlTools sqlTools) {
        this.connectionInfo = connectionInfo;
        this.sqlTools = sqlTools;
    }

    @Override
    public void run() {
        SinkRecordObject sinkRecordObject = null;
        try (Connection connection = connectionInfo.createMysqlConnection();
             Statement statement = connection.createStatement()) {
            while(true){
                try {
                    sinkRecordObject = sinkRecordQueue.take();
                    String sql = constructSql(sinkRecordObject);
                    statement.executeUpdate(sql);
                    count++;
                } catch (InterruptedException exp) {
                    LOGGER.warn("Interrupted exception occurred", exp);
                }
            }
        } catch (SQLException exp) {
            LOGGER.error("SQL exception occurred in work thread", exp);
        }
    }

    /**
     * Adds data
     *
     * @param sinkRecordObject SinkRecordObject the sinkRecordObject
     */
    public void addData(SinkRecordObject sinkRecordObject) {
        sinkRecordQueue.add(sinkRecordObject);
    }

    /**
     * Constructs sql
     *
     * @param sinkRecordObject SinkRecordObject the sinkRecordObject
     * @return String the sql
     */
    public String constructSql(SinkRecordObject sinkRecordObject){
        SourceField sourceField = sinkRecordObject.getSourceField();
        DmlOperation dmlOperation = sinkRecordObject.getDmlOperation();
        String operation = dmlOperation.getOperation();
        TableMetaData tableMetaData;
        if (oldTableMap.containsKey(sourceField.getTable())){
            tableMetaData = oldTableMap.get(sourceField.getTable());
        } else {
            tableMetaData = sqlTools.getTableMetaData(sourceField.getDatabase(),sourceField.getTable());
            oldTableMap.put(sourceField.getTable(), tableMetaData);
        }
        String sql = "";
        switch (operation){
            case INSERT:
                sql = sqlTools.getInsertSql(tableMetaData, dmlOperation.getAfter());
                break;
            case UPDATE:
                sql = sqlTools.getUpdateSql(tableMetaData, dmlOperation.getBefore(), dmlOperation.getAfter());
                break;
            case DELETE:
                sql = sqlTools.getDeleteSql(tableMetaData, dmlOperation.getBefore());
                break;
            default:
                break;
        }
        return sql;
    }

}

