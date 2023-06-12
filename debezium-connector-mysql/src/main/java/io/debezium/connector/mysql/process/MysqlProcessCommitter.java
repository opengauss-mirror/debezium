/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.process;

import io.debezium.connector.mysql.MySqlConnection;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.connector.mysql.sink.task.MySqlSinkConnectorConfig;
import io.debezium.connector.process.BaseProcessCommitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.sql.SQLException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Description: MysqlProcessCommitter
 *
 * @author wangzhengyuan
 * @since  2023-03-20
 */
public class MysqlProcessCommitter extends BaseProcessCommitter {
    /**
     * current event index
     */
    public static long currentEventIndex;
    private static final Logger LOGGER = LoggerFactory.getLogger(MysqlProcessCommitter.class);
    private static final String SHOW_MASTER_STATUS = "SHOW MASTER STATUS";
    private static final String SHOW_SLAVE_STATUS = "SHOW SLAVE STATUS";
    private static final String SHOW_UUID = "show global variables like 'server_uuid'";
    private static final String SHOW_READ_ONLY = "show variables like 'read_only'";
    private static final String VALUE = "Value";
    private static final String MASTER_UUID = "Master_UUID";
    private static final String FORWARD_SOURCE_PROCESS_PREFIX = "forward-source-process-";
    private static final String FORWARD_SINK_PROCESS_PREFIX = "forward-sink-process-";
    private static final String CREATE_COUNT_INFO_NAME = "start-event-index.txt";
    private static final String GTID = "Executed_Gtid_Set";

    private final ThreadPoolExecutor threadPool = new ThreadPoolExecutor(1, 1, 100,
            TimeUnit.SECONDS, new LinkedBlockingQueue<>(1));
    private MysqlSourceProcessInfo sourceProcessInfo;
    private MysqlSinkProcessInfo sinkProcessInfo;
    private MySqlConnection mysqlConnection;
    private long startEventIndex;
    private String uuid;

    /**
     * Constructor
     *
     * @param connectorConfig MySqlConnectorConfig the mySqlConnectorConfig
     * @param originGtidSet String the origin gtid set
     * @param connection MySqlConnection the connection
     */
    public MysqlProcessCommitter(MySqlConnectorConfig connectorConfig, String originGtidSet,
                                MySqlConnection connection) {
        super(connectorConfig, FORWARD_SOURCE_PROCESS_PREFIX);
        this.mysqlConnection = connection;
        this.startEventIndex = initStartEventIndex(originGtidSet);
        executeOutPutThread(connectorConfig.createCountInfoPath() + File.separator);
    }

    /**
     * Constructor
     *
     * @param connectorConfig MySqlSinkConnectorConfig the connectorConfig
     */
    public MysqlProcessCommitter(MySqlSinkConnectorConfig connectorConfig) {
        super(connectorConfig, FORWARD_SINK_PROCESS_PREFIX);
    }

    /**
     * Constructor
     *
     * @param failSqlPath String the fail sql path
     * @param fileSize int the file size
     */
    public MysqlProcessCommitter(String failSqlPath, int fileSize) {
        super(failSqlPath, fileSize);
    }

    /**
     * statSourceProcessInfo
     *
     * @return MysqlSourceProcessInfo the mysqlSourceProcessInfo
     */
    protected MysqlSourceProcessInfo statSourceProcessInfo() {
        long before = waitTimeInterval(true);
        sourceProcessInfo = MysqlSourceProcessInfo.SOURCE_PROCESS_INFO;
        sourceProcessInfo.setSpeed(before, commitTimeInterval);
        sourceProcessInfo.setCreateCount(getCurrentEventIndex() - startEventIndex);
        sourceProcessInfo.setRest(0);
        sourceProcessInfo.setTimestamp();
        return sourceProcessInfo;
    }

    /**
     * statSinkProcessInfo
     *
     * @return MysqlSinkProcessInfo the mysqlSinkProcessInfo
     */
    protected MysqlSinkProcessInfo statSinkProcessInfo() {
        long before = waitTimeInterval(false);
        sinkProcessInfo = MysqlSinkProcessInfo.SINK_PROCESS_INFO;
        sinkProcessInfo.setSpeed(before, commitTimeInterval);
        sinkProcessInfo.setRest(sinkProcessInfo.getSkippedExcludeEventCount(), sinkProcessInfo.getSkippedCount());
        sinkProcessInfo.setTimestamp();
        listenStartEventIndex();
        if (startEventIndex != -1L && currentEventIndex != 0L) {
            sinkProcessInfo.setOverallPipe(currentEventIndex - startEventIndex);
        }
        return sinkProcessInfo;
    }

    private void listenStartEventIndex() {
        startEventIndex = inputCreateCount(createCountInfoPath + File.separator
                + CREATE_COUNT_INFO_NAME, false);
    }

    private long initStartEventIndex(String originGtidSet) {
        initUuid();
        String validGtid = getValidGtid(originGtidSet.split(","));
        String tid = validGtid.split(":")[validGtid.split(":").length - 1];
        return Long.parseLong(tid.split("-")[tid.split("-").length - 1]);
    }

    private void initUuid() {
        AtomicReference<String> readOnlySet = new AtomicReference<>("");
        AtomicReference<String> uuidSet = new AtomicReference<>("");
        try {
            mysqlConnection.query(SHOW_READ_ONLY, rs -> {
                if (rs.next()) {
                    readOnlySet.set(rs.getString(VALUE));
                }
            });
            if ("OFF".equalsIgnoreCase(readOnlySet.get())) {
                mysqlConnection.query(SHOW_UUID, rs -> {
                    if (rs.next()) {
                        uuidSet.set(rs.getString(VALUE));
                    }
                });
            } else {
                mysqlConnection.query(SHOW_SLAVE_STATUS, rs -> {
                    if (rs.next()) {
                        uuidSet.set(rs.getString(MASTER_UUID));
                    }
                });
            }
        } catch (SQLException e) {
            LOGGER.warn("SQL exception occurred.", e);
        }
        this.uuid = uuidSet.get();
    }

    private String getCurrentGtid() throws SQLException {
        AtomicReference<String> gtidSet = new AtomicReference<>("");
        mysqlConnection.query(SHOW_MASTER_STATUS, rs -> {
            if (rs.next()) {
                gtidSet.set(rs.getString(GTID));
            }
        });
        return getValidGtid(gtidSet.get().replaceAll("\\s*", "").split(","));
    }

    private long getCurrentEventIndex() {
        String gtid;
        try {
            gtid = getCurrentGtid();
        } catch (SQLException e) {
            return -1L;
        }
        if (!"".equals(gtid)) {
            String tid = gtid.split(":")[gtid.split(":").length - 1];
            return Long.parseLong(tid.split("-")[tid.split("-").length - 1]);
        }
        return -1L;
    }

    private long waitTimeInterval(boolean isSource) {
        long before;
        if (isSource) {
            sourceProcessInfo = MysqlSourceProcessInfo.SOURCE_PROCESS_INFO;
            before = sourceProcessInfo.getPollCount();
        } else {
            sinkProcessInfo = MysqlSinkProcessInfo.SINK_PROCESS_INFO;
            before = sinkProcessInfo.getReplayedCount();
        }
        try {
            Thread.sleep(commitTimeInterval * 1000L);
        } catch (InterruptedException exp) {
            LOGGER.warn("Interrupted exception occurred", exp);
        }
        return before;
    }

    private String getValidGtid(String[] gtids) {
        for (int i = 0; i < gtids.length; i++) {
            if (gtids[i].split(":")[0].equals(uuid)) {
                gtids[0] = gtids[i];
                break;
            }
        }
        return gtids[0];
    }

    private void executeOutPutThread(String dirPath) {
        threadPool.execute(() -> {
            if (!initFile(dirPath).exists()) {
                LOGGER.warn("Failed to output source create count, the sink overallPipe will always be 0.");
                threadPool.shutdown();
                return;
            }
            outputCreateCountInfo(dirPath + CREATE_COUNT_INFO_NAME,
                    startEventIndex);
            threadPool.shutdown();
        });
    }
}