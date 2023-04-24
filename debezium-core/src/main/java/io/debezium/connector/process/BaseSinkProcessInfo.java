/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.process;

/**
 * Description: BaseSinkProcessCommitter
 *
 * @author wangzhengyuan
 * @since 2023-03-31
 */
public class BaseSinkProcessInfo {
    /**
     * overallPipe
     */
    protected long overallPipe;
    private long timestamp;
    private long extractCount;
    private long replayedCount;
    private long successCount;
    private long failCount;
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
     * get extract count
     *
     * @return Long the extract count
     */
    public long getExtractCount() {
        return extractCount;
    }

    /**
     * set extract count
     *
     * @param extractCount Long the extract count
     */
    public void setExtractCount(long extractCount) {
        this.extractCount = extractCount;
    }

    /**
     * get replayed count
     *
     * @return Long the replayed count
     */
    public long getReplayedCount() {
        return replayedCount;
    }

    /**
     * set replayed count
     *
     * @param replayedCount Long the replayed count
     */
    public void setReplayedCount(long replayedCount) {
        this.replayedCount = replayedCount;
    }

    /**
     * get count of which replayed successfully
     *
     * @return Long the successful count
     */
    public long getSuccessCount() {
        return successCount;
    }

    /**
     * set success count
     *
     * @param successCount Long the success count
     */
    public void setSuccessCount(long successCount) {
        this.successCount = successCount;
    }

    /**
     * get fail count
     *
     * @return Long the fail count
     */
    public long getFailCount() {
        return failCount;
    }

    /**
     * set fail count
     *
     * @param failCount Long the fail count
     */
    public void setFailCount(long failCount) {
        this.failCount = failCount;
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
     * Sets rest
     *
     * @param skippedExcludeCount Long the skipped exclude event count
     * @param skippedCount Long the skipped count
     */
    public void setRest(long skippedExcludeCount, long skippedCount) {
        this.rest = extractCount - replayedCount - skippedExcludeCount - skippedCount;
    }

    /**
     * get speed
     *
     * @return Long the speed
     */
    public long getSpeed() {
        return speed;
    }

    /**
     * Sets peed
     *
     * @param before Long the before
     * @param timeInterval Long the time interval
     */
    public void setSpeed(long before, int timeInterval) {
        this.speed = (replayedCount - before) / timeInterval;
    }

    /**
     * get overall pipe
     *
     * @return Long the overall pipe
     */
    public long getOverallPipe() {
        return overallPipe;
    }
}
