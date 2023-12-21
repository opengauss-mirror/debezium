/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.process;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Description: BaseSourceProcessCommitter
 *
 * @author wangzhengyuan
 * @since 2023-03-31
 */
public class BaseSourceProcessInfo implements Cloneable {
    /**
     * SourceProcessInfo the sourceProcessInfo
     */
    public static final BaseSourceProcessInfo TABLE_SOURCE_PROCESS_INFO = new BaseSourceProcessInfo();

    /**
     * SourceProcessInfo the transaction source process information
     */
    public static final BaseSourceProcessInfo TRANSACTION_SOURCE_PROCESS_INFO = new BaseSourceProcessInfo();
    private static final Logger LOGGER = LoggerFactory.getLogger(BaseSourceProcessInfo.class);

    private final AtomicLong createCount = new AtomicLong();
    private long timestamp;
    private long skippedExcludeCount;
    private long convertCount;
    private long pollCount;
    private long rest;
    private long speed;

    private BaseSourceProcessInfo() {}

    /**
     * get timestamp
     *
     * @return Long the timestamp
     */
    public long getTimestamp() {
        return timestamp;
    }

    /**
     * set timestamp
     */
    public void setTimestamp() {
        this.timestamp = System.currentTimeMillis();
    }

    /**
     * get create count
     *
     * @return Long the create count
     */
    public long getCreateCount() {
        return createCount.get();
    }

    /**
     * set create count
     *
     * @param createCount Long the create count
     */
    public void setCreateCount(long createCount) {
        this.createCount.set(createCount);
    }

    /**
     * get convert count
     *
     * @return Long the convert count
     */
    public long getConvertCount() {
        return convertCount;
    }

    /**
     * set convert count
     *
     * @param convertCount Long the convert count
     */
    public void setConvertCount(long convertCount) {
        this.convertCount = convertCount;
    }

    /**
     * get poll count
     *
     * @return Long the poll count
     */
    public long getPollCount() {
        return pollCount;
    }

    /**
     * set poll count
     *
     * @param pollCount Long the poll count
     */
    public void setPollCount(long pollCount) {
        this.pollCount = pollCount;
    }

    /**
     * get rest
     *
     * @return Long the rest
     */
    public long getRest() {
        return rest;
    }

    public void setRest() {
        this.rest = createCount.get() - pollCount - skippedExcludeCount;
    }

    /**
     * Gets speed
     *
     * @return Long the speed
     */
    public long getSpeed() {
        return speed;
    }

    /**
     * Sets speed
     *
     * @param before Long the before
     * @param timeInterval Int the time interval
     */
    public void setSpeed(long before, int timeInterval) {
        this.speed = (pollCount - before) / timeInterval;
    }

    /**
     * get skipped exclude count
     *
     * @return Long the skipped exclude count
     */
    public long getSkippedExcludeCount() {
        return skippedExcludeCount;
    }

    /**
     * Create count increase automatically based autoIncreaseSize
     *
     * @param autoIncreaseSize int the autoIncreaseSize
     */
    public void autoIncreaseCreateCount(int autoIncreaseSize) {
        createCount.addAndGet(autoIncreaseSize);
    }

    /**
     * Skipped exclude count increase automatically based autoIncreaseSize
     *
     * @param autoIncreaseSize int the autoIncreaseSize
     */
    public void autoIncreaseSkippedExcludeCount(int autoIncreaseSize) {
        skippedExcludeCount += autoIncreaseSize;
    }

    /**
     * Convert count increase automatically based autoIncreaseSize
     *
     * @param autoIncreaseSize int the autoIncreaseSize
     */
    public void autoIncreaseConvertCount(int autoIncreaseSize) {
        convertCount += autoIncreaseSize;
    }

    /**
     * Poll count increase automatically based autoIncreaseSize
     *
     * @param autoIncreaseSize int the autoIncreaseSize
     */
    public void autoIncreasePollCount(int autoIncreaseSize) {
        pollCount += autoIncreaseSize;
    }

    @Override
    public BaseSourceProcessInfo clone() {
        try {
            if (super.clone() instanceof BaseSourceProcessInfo) {
                return (BaseSourceProcessInfo) super.clone();
            }
        } catch (CloneNotSupportedException e) {
            LOGGER.error("The process information object is not supported clone.");
        }
        return this;
    }

    /**
     * Stat convert count, poll count and skipped count
     *
     * @param processInfo BaseProcessInfo the processInfo, either transaction process information
     *                   or table data process information
     * @param executeCount Transaction or data count which has been converted and will be sent to kafka
     * @param ignoreCount Transaction or data count which will be skipped, this means the binlog update
     *                   comes from those tables which in blacklist or not int whitelist
     */
    public static void statProcessCount(BaseSourceProcessInfo processInfo, int executeCount, int ignoreCount) {
        processInfo.autoIncreaseConvertCount(executeCount);
        processInfo.autoIncreasePollCount(executeCount);
        processInfo.autoIncreaseSkippedExcludeCount(ignoreCount);
    }

    @Override
    public String toString() {
        return "{"
                + "\"timestamp\":" + timestamp
                + ",\"createCount\":" + createCount
                + ",\"skippedExcludeCount\":" + skippedExcludeCount
                + ",\"convertCount\":" + convertCount
                + ",\"pollCount\":" + pollCount
                + ",\"speed\":" + speed
                + ",\"rest\":" + rest
                + '}';
    }
}
