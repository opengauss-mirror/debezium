/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.sink.replay;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ConcurrentLinkedQueue;

import io.debezium.connector.mysql.process.MysqlProcessCommitter;
import io.debezium.connector.mysql.process.MysqlSinkProcessInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.mysql.sink.object.ConnectionInfo;
import io.debezium.connector.mysql.sink.object.Transaction;

/**
 * Description: TransactionDispatcher class
 *
 * @author douxin
 * @since 2022-11-02
 **/
public class TransactionDispatcher {
    /**
     * Default max thread count
     */
    public static final int MAX_THREAD_COUNT = 30;

    private static final Logger LOGGER = LoggerFactory.getLogger(TransactionDispatcher.class);

    private int threadCount;
    private int count = 0;
    private ConnectionInfo connectionInfo;
    private Transaction selectedTransaction = null;
    private ArrayList<WorkThread> threadList = new ArrayList<>();
    private final ThreadPoolExecutor threadPool = new ThreadPoolExecutor(1, 1, 100,
            TimeUnit.SECONDS, new LinkedBlockingQueue<>(3));
    private final DateTimeFormatter ofPattern = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
    private ArrayList<String> changedTableNameList;
    private BlockingQueue<String> feedBackQueue;
    private ArrayList<ConcurrentLinkedQueue<Transaction>> transactionQueueList;
    private MysqlProcessCommitter processCommitter;

    /**
     * Constructor
     *
     * @param ConnectionInfo the connection info
     * @param ArrayList<ConcurrentLinkedQueue<Transaction>> the transaction queue list
     * @param ArrayList<String> the changed table name list
     * @param BlockingQueue<String> the feed back queue
     */
    public TransactionDispatcher(ConnectionInfo connectionInfo, ArrayList<ConcurrentLinkedQueue<Transaction>> transactionQueueList,
                                 ArrayList<String> changedTableNameList, BlockingQueue<String> feedBackQueue) {
        this.threadCount = MAX_THREAD_COUNT;
        this.connectionInfo = connectionInfo;
        this.transactionQueueList = transactionQueueList;
        this.changedTableNameList = changedTableNameList;
        this.feedBackQueue = feedBackQueue;
    }

    /**
     * Constructor
     *
     * @param int threadCount
     * @param ConnectionInfo the connection info
     * @param ArrayList<ConcurrentLinkedQueue<Transaction>> the transaction queue list
     * @param ArrayList<String> the changed table name list
     * @param BlockingQueue<String> the feed back queue
     */
    public TransactionDispatcher(int threadCount, ConnectionInfo connectionInfo, ArrayList<ConcurrentLinkedQueue<Transaction>> transactionQueueList,
                                 ArrayList<String> changedTableNameList, BlockingQueue<String> feedBackQueue) {
        this.threadCount = threadCount;
        this.connectionInfo = connectionInfo;
        this.transactionQueueList = transactionQueueList;
        this.changedTableNameList = changedTableNameList;
        this.feedBackQueue = feedBackQueue;
    }

    /**
     * Sets processCommitter
     *
     * @param failSqlPath String the fail sql path
     */
    public void initProcessCommitter(String failSqlPath, int fileSizeLimit) {
        this.processCommitter = new MysqlProcessCommitter(failSqlPath, fileSizeLimit);
    }

    /**
     * Dispatcher
     */
    public void dispatcher() {
        createThreads();
        statTask();
        statReplayTask();
        Transaction txn = null;
        int freeThreadIndex = -1;
        int queueIndex = 0;
        while (true) {
            if (selectedTransaction == null) {
                txn = transactionQueueList.get(queueIndex).poll();
                if (txn != null) {
                    if (LOGGER.isInfoEnabled()) {
                        String txnString = txn.toString();
                        LOGGER.info("Ready to replay the transaction: {}",
                                txnString.substring(0, Math.min(2048, txnString.length())));
                    }
                    count++;
                    if (count % JdbcDbWriter.MAX_VALUE == 0) {
                        queueIndex++;
                        if (queueIndex % JdbcDbWriter.TRANSACTION_QUEUE_NUM == 0) {
                            queueIndex = 0;
                        }
                    }
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

    private int[] getSuccessAndFailCount() {
        int successCount = 0;
        int failCount = 0;
        for (WorkThread workThread : threadList) {
            successCount += workThread.getSuccessCount();
            failCount += workThread.getFailCount();
        }
        return new int[]{successCount, failCount, successCount + failCount};
    }

    private void statReplayTask() {
        threadPool.execute(() -> {
            while (true) {
                MysqlSinkProcessInfo.SINK_PROCESS_INFO.setSuccessCount(getSuccessAndFailCount()[0]);
                MysqlSinkProcessInfo.SINK_PROCESS_INFO.setFailCount(getSuccessAndFailCount()[1]);
                MysqlSinkProcessInfo.SINK_PROCESS_INFO.setReplayedCount(getSuccessAndFailCount()[2]);
                List<String> failSqlList = collectFailSql();
                if (failSqlList.size() > 0) {
                    commitFailSql(failSqlList);
                }
            }
        });
    }

    private List<String> collectFailSql() {
        List<String> failSqlList = new ArrayList<>();
        for (WorkThread workThread : threadList) {
            if (workThread.getFailSqlList().size() != 0) {
                failSqlList.addAll(workThread.getFailSqlList());
                workThread.clearFailSqlList();
            }
        }
        return failSqlList;
    }

    private void commitFailSql(List<String> failSqlList) {
        for (String sql : failSqlList) {
            processCommitter.commitFailSql(sql);
        }
    }

    private void createThreads() {
        for (int i = 0; i < threadCount; i++) {
            WorkThread workThread = new WorkThread(connectionInfo, changedTableNameList, feedBackQueue, i);
            threadList.add(workThread);
            workThread.start();
        }
    }

    private void statTask() {
        Timer timer = new Timer();
        final int[] before = { count };
        TimerTask task = new TimerTask() {
            @Override
            public void run() {
                String date = ofPattern.format(LocalDateTime.now());
                String result = String.format("have replayed %s transaction, and current time is %s, and current "
                        + "speed is %s", count, date, count - before[0]);
                LOGGER.warn(result);
                before[0] = count;
            }
        };
        timer.schedule(task, 1000, 1000);
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
