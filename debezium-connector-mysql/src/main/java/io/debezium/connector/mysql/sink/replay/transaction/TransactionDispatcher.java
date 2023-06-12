/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.sink.replay.transaction;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.breakpoint.BreakPointRecord;
import io.debezium.connector.mysql.process.MysqlProcessCommitter;
import io.debezium.connector.mysql.process.MysqlSinkProcessInfo;
import io.debezium.connector.mysql.sink.object.ConnectionInfo;
import io.debezium.connector.mysql.sink.object.Transaction;
import io.debezium.connector.mysql.sink.object.TableMetaData;
import io.debezium.connector.mysql.sink.util.SqlTools;

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
    private static final int BREAKPOINT_REPEAT_COUNT_LIMIT = 3000;

    private int threadCount;
    private int count = 0;
    private int previousCount = 0;
    private ConnectionInfo connectionInfo;
    private Transaction selectedTransaction = null;
    private ArrayList<WorkThread> threadList = new ArrayList<>();
    private final ThreadPoolExecutor threadPool = new ThreadPoolExecutor(2, 2, 100,
            TimeUnit.SECONDS, new LinkedBlockingQueue<>(3));
    private final DateTimeFormatter ofPattern = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
    private SqlTools sqlTools;
    private List<Long> txnReplayOffsets = new ArrayList<>();
    private BlockingQueue<String> feedBackQueue;
    private ArrayList<ConcurrentLinkedQueue<Transaction>> transactionQueueList;
    private MysqlProcessCommitter processCommitter;
    private BreakPointRecord breakPointRecord;
    private boolean isStop = false;
    private Boolean isBpCondition = false;
    private int filterCount = 0;

    /**
     * Constructor
     *
     * @param ConnectionInfo the connection info
     * @param ArrayList<ConcurrentLinkedQueue<Transaction>> the transaction queue list
     * @param sqlTools sqlTools SqlTools the sql tools
     * @param BlockingQueue<String> the feed back queue
     */
    public TransactionDispatcher(ConnectionInfo connectionInfo, ArrayList<ConcurrentLinkedQueue<Transaction>> transactionQueueList,
                                 SqlTools sqlTools, BlockingQueue<String> feedBackQueue) {
        this.threadCount = MAX_THREAD_COUNT;
        this.connectionInfo = connectionInfo;
        this.transactionQueueList = transactionQueueList;
        this.sqlTools = sqlTools;
        this.feedBackQueue = feedBackQueue;
    }

    /**
     * Constructor
     *
     * @param int threadCount
     * @param ConnectionInfo the connection info
     * @param ArrayList<ConcurrentLinkedQueue<Transaction>> the transaction queue list
     * @param sqlTools SqlTools the sql tools
     * @param BlockingQueue<String> the feed back queue
     */
    public TransactionDispatcher(int threadCount, ConnectionInfo connectionInfo, ArrayList<ConcurrentLinkedQueue<Transaction>> transactionQueueList,
                                 SqlTools sqlTools, BlockingQueue<String> feedBackQueue) {
        this.threadCount = threadCount;
        this.connectionInfo = connectionInfo;
        this.transactionQueueList = transactionQueueList;
        this.sqlTools = sqlTools;
        this.feedBackQueue = feedBackQueue;
    }

    /**
     * Sets breakpointRecord
     *
     * @param breakPointRecord the breakpoint record
     */
    public void initBreakPointRecord(BreakPointRecord breakPointRecord) {
        this.breakPointRecord = breakPointRecord;
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
     * Sets the isStop.
     *
     * @param isStop boolean invoke stop
     */
    public void setIsStop(boolean isStop) {
        this.isStop = isStop;
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
     * Sets the isBpCondition.
     *
     * @param isBpCondition the value of the isBpCondition
     */
    public void setIsBpCondition(Boolean isBpCondition) {
        this.isBpCondition = isBpCondition;
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
        while (!isStop) {
            if (selectedTransaction == null) {
                txn = transactionQueueList.get(queueIndex).poll();
                if (txn != null) {
                    txn = filterByDb(txn);
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

    private Transaction filterByDb(Transaction txn) {
        Transaction transaction = txn;
        if (isBpCondition && filterCount < BREAKPOINT_REPEAT_COUNT_LIMIT) {
            filterCount++;
            if (isSkipTxn(txn)) {
                LOGGER.info("The txn is already replay, "
                        + "so skip this txn that gtid is {}", txn.getSourceField().getGtid());
                addReplayedOffset(txn, breakPointRecord.getReplayedOffset());
                transaction = null;
            }
        }
        return transaction;
    }

    /**
     * Add fail transaction count
     */
    public void addFailTransaction() {
        count++;
        threadList.get(0).addFailTransaction();
    }

    private boolean isSkipTxn(Transaction txn) {
        String firstSql = txn.getSqlList().get(0);
        StringBuilder sb = new StringBuilder();
        String builtSql;
        if (txn.getIsDml()) {
            if (firstSql.contains("insert into")) {
                sb.append("select * from ");
                String[] insertSql = firstSql.split(" ");
                String schemaName = insertSql[2].split("\\.")[0].replace("\"", "");
                String tableName = insertSql[2].split("\\.")[1].replace("\"", "");
                sb.append(schemaName).append(".").append(tableName);
                sb.append(" where ");
                TableMetaData tableMetaData = sqlTools.getTableMetaData(schemaName, tableName);
                String valueString = firstSql.substring(firstSql.indexOf("("));
                String[] columnValueArray = valueString.replace("(", "")
                        .replace(")", "").split(", ");
                ArrayList<String> valueList = new ArrayList<>();
                for (int i = 0; i < columnValueArray.length; i++) {
                    valueList.add(tableMetaData.getColumnList().get(i).getColumnName() + "=" + columnValueArray[i]);
                }
                sb.append(String.join(" and ", valueList));
                builtSql = sb.toString();
                return sqlTools.isExistSql(builtSql);
            } else if (firstSql.contains("update")) {
                int setIndex = firstSql.indexOf("set");
                String schemaAndTable = firstSql.substring(7, setIndex);
                sb.append("select * from ").append(schemaAndTable).append(" where ");
                int whereIndex = firstSql.indexOf("where");
                String condition = firstSql.substring(setIndex + 4, whereIndex)
                        .replace(",", " and")
                        + "and" + firstSql.substring(whereIndex + 5);
                sb.append(condition);
                builtSql = sb.toString();
                return sqlTools.isExistSql(builtSql);
            } else if (firstSql.contains("delete from")) {
                builtSql = firstSql.replace("delete from", "select * from");
                return sqlTools.isExistSql(builtSql);
            } else {
                return false;
            }
        }
        return false;
    }

    private void addReplayedOffset(Transaction txn, PriorityBlockingQueue<Long> txnReplayedOffsets) {
        for (long i = txn.getTxnBeginOffset(); i <= txn.getTxnEndOffset(); i++) {
            txnReplayedOffsets.add(i);
        }
        txnReplayedOffsets.addAll(txnReplayOffsets);
        txnReplayOffsets.clear();
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
        return new int[]{ successCount, failCount, successCount + failCount };
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
                try {
                    Thread.sleep(1000);
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
            WorkThread workThread = new WorkThread(connectionInfo, feedBackQueue,
                    i, breakPointRecord);
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
