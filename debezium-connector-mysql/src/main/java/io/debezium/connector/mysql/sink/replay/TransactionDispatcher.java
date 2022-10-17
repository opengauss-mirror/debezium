/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.sink.replay;

import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.concurrent.BlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.mysql.sink.object.ConnectionInfo;
import io.debezium.connector.mysql.sink.object.Transaction;

/**
 * Description: TransactionDispatcher class
 * @author douxin
 * @date 2022/11/02
 **/
public class TransactionDispatcher {
    /**
     * Default max thread count
     */
    public static final int MAX_THREAD_COUNT = 50;

    private static final Logger LOGGER = LoggerFactory.getLogger(TransactionDispatcher.class);

    private int threadCount;
    private int count = 0;
    private ConnectionInfo connectionInfo;
    private Transaction selectedTransaction = null;
    private ArrayList<WorkThread> threadList = new ArrayList<>();
    private final SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    private final DateTimeFormatter ofPattern = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
    private BlockingQueue<Transaction> transactionQueue;
    private ArrayList<String> changedTableNameList;
    private BlockingQueue<String> feedBackQueue;

    /**
     * Constructor
     *
     * @param ConnectionInfo the connection info
     * @param BlockingQueue<Transaction> the transaction queue
     * @param ArrayList<String> the changed table name list
     * @param BlockingQueue<String> the feed back queue
     */
    public TransactionDispatcher(ConnectionInfo connectionInfo, BlockingQueue<Transaction> transactionQueue,
                                 ArrayList<String> changedTableNameList, BlockingQueue<String> feedBackQueue) {
        this.threadCount = MAX_THREAD_COUNT;
        this.connectionInfo = connectionInfo;
        this.transactionQueue = transactionQueue;
        this.changedTableNameList = changedTableNameList;
        this.feedBackQueue = feedBackQueue;
    }

    /**
     * Dispatcher
     */
    public void dispatcher() {
        createThreads();
        statTask();
        Transaction txn = null;
        int freeThreadIndex = -1;
        while (true) {
            if (selectedTransaction == null) {
                try {
                    txn = transactionQueue.take();
                }
                catch (InterruptedException exp) {
                    LOGGER.warn("Interrupted exception occurred", exp);
                }
                if (txn != null) {
                    count++;
                }
                else {
                    try {
                        Thread.sleep(1);
                    }
                    catch (InterruptedException exp) {
                        LOGGER.warn("Interrupted exception occurred", exp);
                    }
                }
            }
            else {
                txn = selectedTransaction;
                selectedTransaction = null;
            }
            if (txn != null) {
                freeThreadIndex = canParallelAndFindFreeThread(txn, threadList);
                if (freeThreadIndex == -1) {
                    selectedTransaction = txn;
                }
                else {
                    threadList.get(freeThreadIndex).resumeThread(txn);
                }
            }
        }
    }

    private void createThreads() {
        for (int i = 0; i < threadCount; i++) {
            WorkThread workThread = new WorkThread(connectionInfo, changedTableNameList, feedBackQueue);
            threadList.add(workThread);
            workThread.start();
        }
    }

    private void statTask() {
        new Thread(() -> {
            int before = count;
            int delta = 0;
            while (true) {
                try {
                    Thread.sleep(1000);
                    delta = count - before;
                    before = count;
                    String date = ofPattern.format(LocalDateTime.now());
                    String result = String.format("have replayed %s transaction, and current time is %s, and current " +
                            "speed is %s", count, date, delta);
                    LOGGER.info(result);
                }
                catch (InterruptedException exp) {
                    LOGGER.warn("Interrupted exception occurred", exp);
                }
            }
        }).start();
    }

    private int canParallelAndFindFreeThread(Transaction transaction, ArrayList<WorkThread> threadList) {
        int freeThreadIndex = -1;
        for (int i = 0; i < threadCount; i++) {
            Transaction runningTransaction = threadList.get(i).getTransaction();
            if (runningTransaction != null) {
                boolean canParallel = transaction.interleaved(runningTransaction);
                if (!canParallel) {
                    return -1;
                }
            }
            else {
                freeThreadIndex = i;
            }
        }
        return freeThreadIndex;
    }
}
