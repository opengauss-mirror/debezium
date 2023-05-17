/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.process;

/**
 * Description: BaseSourceProcessCommitter
 *
 * @author wangzhengyuan
 * @since 2023-03-31
 */
public class BaseSourceProcessInfo {
    private long timestamp;
    private long createCount;
    private long convertCount;
    private long pollCount;
    private long rest;
    private long speed;

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
        return createCount;
    }

    /**
     * set create count
     *
     * @param createCount Long the create count
     */
    public void setCreateCount(long createCount) {
        this.createCount = createCount;
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

    /**
     * set rest
     *
     * @param skippedCount Long the rest
     */
    public void setRest(long skippedCount) {
        this.rest = createCount - pollCount - skippedCount;
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
}
