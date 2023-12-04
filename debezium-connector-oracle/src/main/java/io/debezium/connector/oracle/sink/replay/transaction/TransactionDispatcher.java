/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.sink.replay.transaction;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.oracle.process.OracleProcessCommitter;
import io.debezium.connector.oracle.process.OracleSinkProcessInfo;
import io.debezium.connector.oracle.sink.object.ConnectionInfo;
import io.debezium.connector.oracle.sink.object.Transaction;

/**
 * Description: TransactionDispatcher class
 *
 * @author gbase
 * @date 2023/07/31
 **/
public class TransactionDispatcher {
    private static final Logger LOGGER = LoggerFactory.getLogger(TransactionDispatcher.class);
    /**
     * Default max thread count (serial replay transaction)
     */
    public static final int MAX_THREAD_COUNT = 1;

    private int threadCount;
    private int count = 0;
    private int previousCount = 0;
    private ConnectionInfo connectionInfo;
    private Transaction selectedTransaction = null;
    private final ArrayList<WorkThread> threadList = new ArrayList<>();
    private final ThreadPoolExecutor threadPool = new ThreadPoolExecutor(1, 1, 100,
            TimeUnit.SECONDS, new LinkedBlockingQueue<>(3));
    private final DateTimeFormatter ofPattern = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
    private BlockingQueue<String> feedBackQueue;
    private ArrayList<ConcurrentLinkedQueue<Transaction>> transactionQueueList;
    private OracleProcessCommitter processCommitter;

    /**
     * Constructor
     *
     * @param connectionInfo the connection info
     * @param transactionQueueList the transaction queue list
     * @param feedBackQueue the feedback queue
     */
    public TransactionDispatcher(ConnectionInfo connectionInfo,
                                 ArrayList<ConcurrentLinkedQueue<Transaction>> transactionQueueList,
                                 BlockingQueue<String> feedBackQueue) {
        this.threadCount = MAX_THREAD_COUNT;
        this.connectionInfo = connectionInfo;
        this.transactionQueueList = transactionQueueList;
        this.feedBackQueue = feedBackQueue;
    }

    /**
     * Sets processCommitter
     *
     * @param failSqlPath String the fail sql path
     * @param fileSizeLimit the file size limit
     */
    public void initProcessCommitter(String failSqlPath, int fileSizeLimit) {
        this.processCommitter = new OracleProcessCommitter(failSqlPath, fileSizeLimit);
    }

    /**
     * Get count
     *
     * @return int the count
     */
    public int getCount() {
        return this.count;
    }

    /**
     * Get previous count
     *
     * @return int the previous count
     */
    public int getPreviousCount() {
        return this.previousCount;
    }

    /**
     * Dispatcher
     */
    public void dispatcher() {
        createThreads();
        statTask();
        statWorkThreadStatusTask();
        statReplayTask();
        Transaction txn = null;
        int queueIndex = 0;
        WorkThread workThread = null;
        while (true) {
            if (selectedTransaction == null) {
                txn = transactionQueueList.get(queueIndex).poll();
                if (txn != null) {
                    count++;
                    if (count % TransactionReplayTask.MAX_VALUE == 0) {
                        queueIndex++;
                        if (queueIndex % TransactionReplayTask.TRANSACTION_QUEUE_NUM == 0) {
                            queueIndex = 0;
                        }
                    }
                }
                else {
                    try {
                        TimeUnit.MILLISECONDS.sleep(1);
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
                workThread = canParallelAndFindFreeThread(txn, threadList);
                if (null == workThread) {
                    selectedTransaction = txn;
                }
                else {
                    if (LOGGER.isInfoEnabled()) {
                        String txnString = txn.toString();
                        LOGGER.info("In {}, ready to replay the transaction: {}", workThread.getName(),
                                txnString.substring(0, Math.min(2048, txnString.length())));
                    }
                    workThread.resumeThread(txn);
                }
            }
        }
    }

    /**
     * Add fail transaction count
     */
    public void addFailTransaction() {
        count++;
        threadList.get(0).addFailTransaction();
    }

    private int[] getSuccessAndFailCount() {
        int successCount = 0;
        int failCount = 0;
        for (WorkThread workThread : threadList) {
            if (workThread.isAlive()) {
                successCount += workThread.getSuccessCount();
                failCount += workThread.getFailCount();
            }
        }
        return new int[]{successCount, failCount, successCount + failCount};
    }

    private void statReplayTask() {
        threadPool.execute(() -> {
            while (true) {
                OracleSinkProcessInfo.SINK_PROCESS_INFO.setSuccessCount(getSuccessAndFailCount()[0]);
                OracleSinkProcessInfo.SINK_PROCESS_INFO.setFailCount(getSuccessAndFailCount()[1]);
                OracleSinkProcessInfo.SINK_PROCESS_INFO.setReplayedCount(getSuccessAndFailCount()[2]);
                List<String> failSqlList = collectFailSql();
                if (failSqlList.size() > 0) {
                    commitFailSql(failSqlList);
                }
                try {
                    TimeUnit.SECONDS.sleep(1);
                }
                catch (InterruptedException e) {
                    LOGGER.error("Interrupted exception occurred while thread sleeping", e);
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
            WorkThread workThread = new WorkThread(connectionInfo, feedBackQueue, i);
            threadList.add(workThread);
            workThread.start();
        }
    }

    private void statTask() {
        Timer timer = new Timer();
        TimerTask task = new TimerTask() {
            @Override
            public void run() {
                Thread.currentThread().setName("timer-replay-txn");
                LOGGER.warn("have replayed {} transaction, and current time {}, and current speed is {}",
                        count, ofPattern.format(LocalDateTime.now()), count - previousCount);
                previousCount = count;
            }
        };
        timer.schedule(task, 1000, 1000);
    }

    private void statWorkThreadStatusTask() {
        Timer timer = new Timer();
        TimerTask task = new TimerTask() {
            @Override
            public void run() {
                Thread.currentThread().setName("timer-work-status");
                for (int i = 0; i < threadList.size(); i++) {
                    if (!threadList.get(i).isAlive()) {
                        LOGGER.error("Total {} work thread, current work thread {} is dead, so remove it.",
                                threadList.size(), i);
                        threadList.remove(i);
                    }
                }
            }
        };
        timer.schedule(task, 0, 1000 * 60 * 5);
    }

    private WorkThread canParallelAndFindFreeThread(Transaction transaction, ArrayList<WorkThread> threadList) {
        WorkThread freeWorkThread = null;
        for (WorkThread workThread : threadList) {
            Transaction runningTransaction = workThread.getTransaction();
            if (runningTransaction != null) {
                boolean canParallel = transaction.interleaved(runningTransaction);
                if (!canParallel) {
                    return null;
                }
            }
            else {
                freeWorkThread = workThread;
            }
        }
        return freeWorkThread;
    }
}
