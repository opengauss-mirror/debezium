/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.sink.replay;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.mysql.sink.object.ConnectionInfo;
import io.debezium.connector.mysql.sink.object.Transaction;
import io.debezium.connector.mysql.sink.util.SqlTools;

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

    private ConnectionInfo connectionInfo;
    private int successCount;
    private int failCount;
    private Transaction txn = null;
    private final Object lock = new Object();
    private ArrayList<String> changedTableNameList;
    private BlockingQueue<String> feedBackQueue;
    private List<String> failSqlList = new ArrayList<>();

    /**
     * Constructor
     *
     * @param connectionInfo Connection the connection
     * @param changedTableNameList ArrayList<String> the changedTableNameList
     * @param feedBackQueue BlockingQueue<String> the feedBackQueue
     * @param index int the index
     */
    public WorkThread(ConnectionInfo connectionInfo, ArrayList<String> changedTableNameList,
                      BlockingQueue<String> feedBackQueue, int index) {
        super("work-thread-" + index);
        this.connectionInfo = connectionInfo;
        this.changedTableNameList = changedTableNameList;
        this.feedBackQueue = feedBackQueue;
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
                lock.wait();
            }
            catch (InterruptedException exp) {
                LOGGER.error("Interrupted exception occurred", exp);
            }
        }
    }

    @Override
    public void run() {
        try (Connection connection = connectionInfo.createOpenGaussConnection();
                Statement statement = connection.createStatement()) {
            while (true) {
                pauseThread();
                boolean isSuccess = true;
                statement.execute(BEGIN);
                for (String sql : txn.getSqlList()) {
                    try {
                        statement.execute(sql);
                    }
                    catch (SQLException exp) {
                        isSuccess = false;
                        LOGGER.error(String.format("SQL exception occurred, the SQL statement executed is: %s,"
                                + " and the cause of the exception is %s",
                                txn.getSqlList(), exp.getMessage()));
                    }
                    finally {
                        feedBackModifiedTable();
                    }
                }
                if (isSuccess) {
                    statement.execute(COMMIT);
                    successCount++;
                }
                else {
                    statement.execute(ROLLBACK);
                    failCount++;
                    failSqlList.addAll(txn.getSqlList());
                }
            }
        }
        catch (SQLException exp) {
            LOGGER.error("SQL exception occurred in work thread", exp);
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
        if (!txn.getIsDml() && SqlTools.isCreateOrAlterTableStatement(txn.getSqlList().get(1))) {
            String schemaName = txn.getSourceField().getDatabase();
            String tableName = txn.getSourceField().getTable();
            String tableFullName = schemaName + "." + tableName;
            feedBackQueue.add(tableFullName);
        }
    }
}
