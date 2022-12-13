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
import java.util.Locale;
import java.util.concurrent.BlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.mysql.sink.object.ConnectionInfo;
import io.debezium.connector.mysql.sink.object.Transaction;

/**
 * Description: WorkThread class
 * @author douxin
 * @date 2022/11/01
 **/
public class WorkThread extends Thread {
    private static final Logger LOGGER = LoggerFactory.getLogger(WorkThread.class);

    private ConnectionInfo connectionInfo;
    private Transaction txn = null;
    private final Object lock = new Object();
    private ArrayList<String> changedTableNameList;
    private BlockingQueue<String> feedBackQueue;

    /**
     * Constructor
     *
     * @param ConnectionInfo the connection info
     * @param ArrayList<String> the changed table name list
     * @param BlockingQueue<String> the feed back queue
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
     * @param Transaction the transaction
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
     * @param Transaction the transaction
     */
    public void resumeThread(Transaction transaction) {
        synchronized (lock) {
            setTransaction(transaction);
            lock.notify();
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
                try {
                    for (String sql : txn.getSqlList()) {
                        statement.execute(sql);
                    }
                    if (!txn.getIsDml() && (txn.getSqlList().get(0).toLowerCase(Locale.ROOT).startsWith("alter table") ||
                            txn.getSqlList().get(0).toLowerCase(Locale.ROOT).startsWith("create table"))) {
                        String schemaName = txn.getSourceField().getDatabase();
                        String tableName = txn.getSourceField().getTable();
                        String tableFullName = schemaName + "." + tableName;
                        feedBackQueue.add(tableFullName);
                    }
                }
                catch (SQLException exp) {
                    LOGGER.error("SQL exception occurred, the SQL statement executed is " + txn.getSqlList());
                }
            }
        }
        catch (SQLException exp) {
            LOGGER.error("SQL exception occurred in work thread", exp);
        }
    }
}
