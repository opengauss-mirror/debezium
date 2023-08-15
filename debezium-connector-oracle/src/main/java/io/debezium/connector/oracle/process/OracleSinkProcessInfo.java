/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.process;

import io.debezium.connector.process.BaseSinkProcessInfo;

/**
 * Description: OracleSinkProcessInfo
 *
 * @author gbase
 * @since 2023-07-28
 */
public class OracleSinkProcessInfo extends BaseSinkProcessInfo {
    /**
     * SinkProcessInfo the sinkProcessInfo
     */
    public static final OracleSinkProcessInfo SINK_PROCESS_INFO = new OracleSinkProcessInfo();

    private long skippedExcludeEventCount;
    private long skippedCount;

    private OracleSinkProcessInfo() {
    }

    /**
     * get skipped count
     *
     * @return Long the skipped count
     */
    public long getSkippedCount() {
        return skippedCount;
    }

    /**
     * set skipped count
     *
     * @param skippedCount Long the skipped count
     */
    public void setSkippedCount(long skippedCount) {
        this.skippedCount = skippedCount;
    }

    /**
     * get skipped exclude event count
     *
     * @return Long the skipped exclude event count
     */
    public long getSkippedExcludeEventCount() {
        return skippedExcludeEventCount;
    }

    /**
     * set skipped exclude event count
     *
     * @param skippedExcludeEventCount Long the skipped exclude event count
     */
    public void setSkippedExcludeEventCount(long skippedExcludeEventCount) {
        this.skippedExcludeEventCount = skippedExcludeEventCount;
    }

    /**
     * set overall pipe
     *
     * @param createCount Long the overall pipe
     */
    public void setOverallPipe(long createCount) {
        this.overallPipe = createCount - getReplayedCount() - skippedCount - skippedExcludeEventCount;
    }
}
