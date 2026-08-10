/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.sink.replay.transaction;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.ThreadExceptionHandler;
import io.debezium.connector.breakpoint.BreakPointObject;
import io.debezium.connector.breakpoint.BreakPointRecord;
import io.debezium.connector.mysql.sink.object.ConnectionInfo;
import io.debezium.connector.mysql.sink.object.Transaction;
import io.debezium.connector.mysql.sink.util.SqlTools;
import io.debezium.enums.ErrorCode;

/**
 * Description: WorkThread class
 *
 * @author douxin
 * @since 2022-11-01
 **/
public class WorkThread extends Thread {
    private static final Logger LOGGER = LoggerFactory.getLogger(WorkThread.class);
    private static final String BEGIN = "begin";
    private static final String COMMIT = "commit";
    private static final String ROLLBACK = "rollback";

    private final DateTimeFormatter sqlPattern = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH:mm:ss.SSS");
    private ConnectionInfo connectionInfo;
    private int successCount;
    private int failCount;
    private Transaction txn = null;
    private final Object lock = new Object();
    private BlockingQueue<String> feedBackQueue;
    private List<String> failSqlList = new ArrayList<>();
    private BreakPointRecord breakPointRecord;
    private PriorityBlockingQueue<Long> replayedOffsets;
    private boolean isTransaction;
    private boolean isConnection = true;
    private boolean isAlive = true;
    private boolean isWaiting = false;

    /**
     * Constructor
     *
     * @param connectionInfo Connection the connection
     * @param feedBackQueue BlockingQueue<String> the feedBackQueue
     * @param index int the index
     * @param breakPointRecord record break point info
     */
    public WorkThread(ConnectionInfo connectionInfo, BlockingQueue<String> feedBackQueue,
                      int index, BreakPointRecord breakPointRecord) {
        super("work-thread-" + index);
        this.connectionInfo = connectionInfo;
        this.feedBackQueue = feedBackQueue;
        this.breakPointRecord = breakPointRecord;
        this.replayedOffsets = breakPointRecord.getReplayedOffset();
        this.isTransaction = true;
    }

    /**
     * Sets transaction
     *
     * @param transaction Transaction the transaction
     */
    public void setTransaction(Transaction transaction) {
        this.txn = transaction;
    }

    /**
     * Gets transaction
     *
     * @return Transaction the transaction
     */
    public Transaction getTransaction() {
        return this.txn;
    }

    /**
     * Clean transaction
     */
    public void cleanTransaction() {
        this.txn = null;
    }

    /**
     * Resume thread
     *
     * @param transaction Transaction the transaction
     */
    public void resumeThread(Transaction transaction) {
        synchronized (lock) {
            setTransaction(transaction);
            lock.notifyAll();
        }
    }

    /**
     * Pause thread
     */
    public void pauseThread() {
        synchronized (lock) {
            try {
                cleanTransaction();
                isWaiting = true;
                lock.wait();
            }
            catch (InterruptedException exp) {
                // Distinguish a normal shutdown (interrupted to wake up from wait) from an abnormal interruption.
                if (isAlive) {
                    LOGGER.error("{}Interrupted exception occurred", ErrorCode.THREAD_INTERRUPTED_EXCEPTION, exp);
                }
                else {
                    LOGGER.info("Work thread {} is interrupted for shutdown", this.getName());
                }
                Thread.currentThread().interrupt();
            }
            finally {
                isWaiting = false;
            }
        }
    }

    /**
     * Check if thread is waiting for new transaction
     */
    public boolean isWaiting() {
        return isWaiting && isAlive && isConnection;
    }

    /**
     * Add fail transaction count
     */
    public void addFailTransaction() {
        failCount++;
    }

    @Override
    public void run() {
        Thread.currentThread().setUncaughtExceptionHandler(new ThreadExceptionHandler());
        Connection connection = null;
        Statement statement = null;
        while (isAlive) {
            try {
                if (connection == null || connection.isClosed()) {
                    connection = connectionInfo.createOpenGaussConnection();
                    statement = connection.createStatement();
                    isConnection = true;
                    LOGGER.info("Work thread {} connected to database successfully", this.getName());
                }
                while (isConnection && isAlive) {
                    pauseThread();
                    if (!isAlive) {
                        break;
                    }
                    if (txn == null) {
                        continue;
                    }
                    replayTransaction(statement, connection);
                }
            }
            catch (Throwable exp) {
                String errorMsg = exp.getMessage() != null ? exp.getMessage() : exp.toString();
                LOGGER.error("{}Exception occurred in work thread {}: {}",
                        ErrorCode.DB_CONNECTION_EXCEPTION, this.getName(), errorMsg, exp);
                if (txn != null) {
                    failCount++;
                    List<String> tmpSqlList = new ArrayList<>();
                    tmpSqlList.add("-- " + sqlPattern.format(LocalDateTime.now()) + ": " + txn.getSourceField());
                    tmpSqlList.add("-- Connection exception: " + errorMsg);
                    tmpSqlList.addAll(txn.getSqlList() != null ? txn.getSqlList() : new ArrayList<>());
                    tmpSqlList.add(System.lineSeparator());
                    failSqlList.addAll(tmpSqlList);
                }
                isConnection = false;
                try {
                    if (statement != null && !statement.isClosed()) {
                        statement.close();
                    }
                    if (connection != null && !connection.isClosed()) {
                        connection.close();
                    }
                }
                catch (SQLException closeEx) {
                    LOGGER.warn("Failed to close connection after error", closeEx);
                }
                statement = null;
                connection = null;
                cleanTransaction();
                if (isAlive) {
                    LOGGER.info("Work thread {} will attempt to reconnect in 5 seconds", this.getName());
                    try {
                        Thread.sleep(5000);
                    }
                    catch (InterruptedException ie) {
                        LOGGER.warn("Reconnect wait interrupted", ie);
                        Thread.currentThread().interrupt();
                    }
                }
            }
        }
        LOGGER.info("Work thread {} is stopping", this.getName());
        try {
            if (statement != null && !statement.isClosed()) {
                statement.close();
            }
            if (connection != null && !connection.isClosed()) {
                connection.close();
            }
        }
        catch (SQLException closeEx) {
            LOGGER.warn("Failed to close connection during shutdown", closeEx);
        }
    }

    private void replayTransaction(Statement statement, Connection connection) {
        if (txn == null) {
            return;
        }
        try {
            boolean shouldStartTransaction = txn.getSqlList() != null && txn.getSqlList().size() > 1;
            if (shouldStartTransaction) {
                statement.execute(BEGIN);
            }
            boolean isSuccess = executeTxnSql(statement, connection);
            if (isSuccess) {
                if (shouldStartTransaction) {
                    statement.execute(COMMIT);
                }
                successCount++;
            }
            else {
                if (shouldStartTransaction && isConnection) {
                    try {
                        statement.execute(ROLLBACK);
                    }
                    catch (SQLException rollbackEx) {
                        LOGGER.warn("Failed to rollback transaction after error", rollbackEx);
                    }
                }
                recordFailSql();
            }
            if (isConnection) {
                buildAndSaveBpInfo();
            }
        }
        catch (SQLException exp) {
            // Transaction control statements (BEGIN/COMMIT) or breakpoint saving failed.
            // An internal error must never block the thread: only mark the transaction as failed and
            // continue with the next one, unless the connection itself is confirmed broken (then reconnect).
            if (!connectionInfo.checkConnectionStatus(connection)) {
                isConnection = false;
            }
            String errorMsg = exp.getMessage() != null ? exp.getMessage() : exp.toString();
            txn.setExpMessage(errorMsg);
            recordFailSql();
        }
        catch (Throwable exp) {
            // Any internal error (e.g. breakpoint persistence) must never block the thread:
            // record the failure and continue processing the next transaction.
            String errorMsg = exp.getMessage() != null ? exp.getMessage() : exp.toString();
            txn.setExpMessage(errorMsg);
            recordFailSql();
        }
        finally {
            cleanTransaction();
        }
    }

    private void recordFailSql() {
        failCount++;
        List<String> tmpSqlList = new ArrayList<>();
        tmpSqlList.add("-- " + sqlPattern.format(LocalDateTime.now()) + ": " + txn.getSourceField());
        tmpSqlList.add("-- " + txn.getExpMessage());
        tmpSqlList.addAll(txn.getSqlList() != null ? txn.getSqlList() : new ArrayList<>());
        tmpSqlList.add(System.lineSeparator());
        failSqlList.addAll(tmpSqlList);
    }

    /**
     * Can the thread be available
     *
     * @return boolean the canUse
     */
    public boolean canUse() {
        return isAlive && isConnection;
    }

    /**
     * Sets alive
     *
     * @param alive boolean the alive
     */
    public void setAlive(boolean alive) {
        isAlive = alive;
    }

    /**
     * Sets the isStop.
     *
     * @param isStop boolean isStop
     */
    public void setIsStop(boolean isStop) {
        if (isStop) {
            this.isAlive = false;
            this.isConnection = false;
            this.interrupt();
        }
    }

    private boolean executeTxnSql(Statement statement, Connection connection) {
        for (String sql : txn.getSqlList()) {
            try {
                statement.execute(sql);
            }
            catch (SQLException exp) {
                String errorMsg = exp.getMessage() != null ? exp.getMessage() : exp.toString();
                if (!connectionInfo.checkConnectionStatus(connection)) {
                    isConnection = false;
                    txn.setExpMessage("Connection failed: " + errorMsg);
                    return false;
                }
                LOGGER.error("{}SQL exception occurred in transaction {}", ErrorCode.SQL_EXCEPTION,
                    txn.getSourceField());
                LOGGER.error("{}The error SQL statement executed is: {}", ErrorCode.SQL_EXCEPTION, sql);
                LOGGER.error("{}the cause of the exception is {}", ErrorCode.SQL_EXCEPTION, errorMsg);
                txn.setExpMessage(errorMsg);
                return false;
            }
            finally {
                feedBackModifiedTable();
            }
        }
        return true;
    }

    private void buildAndSaveBpInfo() {
        if (txn != null) {
            List<String> sqlList = txn.getSqlList();
            if (txn.getIsDml() && sqlList != null && sqlList.size() > 0) {
                replayedOffsets.add(txn.getTxnBeginOffset());
                replayedOffsets.addAll(txn.getSqlOffsets() != null ? txn.getSqlOffsets() : new ArrayList<>());
                replayedOffsets.add(txn.getTxnEndOffset());
            } else {
                replayedOffsets.add(txn.getTxnBeginOffset());
            }
            savedBreakPointInfo(txn);
        }
    }

    /**
     * get success count
     *
     * @return count of replayed successfully
     */
    public int getSuccessCount() {
        return this.successCount;
    }

    /**
     * get fail sql list
     *
     * @return List the fail sql list
     */
    public List<String> getFailSqlList() {
        return failSqlList;
    }

    /**
     * Save breakpoint data to kafka
     *
     * @param txn the replay transaction
     */
    private void savedBreakPointInfo(Transaction txn) {
        BreakPointObject txnBpObject = new BreakPointObject();
        txnBpObject.setBeginOffset(txn.getTxnBeginOffset());
        txnBpObject.setEndOffset(txn.getTxnEndOffset());
        txnBpObject.setTimeStamp(LocalDateTime.now().toString());
        if (!txn.getSourceField().getGtid().isEmpty()) {
            txnBpObject.setGtid(txn.getSourceField().getGtid());
        }
        breakPointRecord.storeRecord(txnBpObject, isTransaction);
    }

    /**
     * clear fail sql list
     */
    public void clearFailSqlList() {
        failSqlList.clear();
    }

    /**
     * get fail count
     *
     * @return int the fail count
     */
    public int getFailCount() {
        return failCount;
    }

    private void feedBackModifiedTable() {
        if (txn == null) {
            return;
        }
        List<String> sqlList = txn.getSqlList();
        if (!txn.getIsDml() && sqlList != null && sqlList.size() > 1
                && SqlTools.isCreateOrAlterTableStatement(sqlList.get(1))) {
            String schemaName = txn.getSourceField() != null ? txn.getSourceField().getDatabase() : null;
            String tableName = txn.getSourceField() != null ? txn.getSourceField().getTable() : null;
            if (schemaName != null && tableName != null) {
                String tableFullName = schemaName + "." + tableName;
                feedBackQueue.add(tableFullName);
            }
        }
    }
}
