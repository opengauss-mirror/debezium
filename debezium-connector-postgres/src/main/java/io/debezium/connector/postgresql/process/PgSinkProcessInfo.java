/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package io.debezium.connector.postgresql.process;

import io.debezium.connector.process.BaseSinkProcessInfo;

/**
 * Description: PgSinkProcessInfo
 *
 * @author jianghongbo
 * @since 2025-02-05
 */
public class PgSinkProcessInfo extends BaseSinkProcessInfo {
    /**
     * SinkProcessInfo the sinkProcessInfo
     */
    public static final PgSinkProcessInfo SINK_PROCESS_INFO = new PgSinkProcessInfo();

    private long skippedExcludeEventCount;
    private long skippedCount;

    private PgSinkProcessInfo() {
    }

    /**
     * set overall pipe
     *
     * @param createCount Long the overall pipe
     */
    public void setOverallPipe(long createCount) {
        long res = createCount - getReplayedCount();
        this.overallPipe = res >= 0 ? res : 0;
    }

    public long getSkippedExcludeEventCount() {
        return skippedExcludeEventCount;
    }

    public void setSkippedExcludeEventCount(long skippedExcludeEventCount) {
        this.skippedExcludeEventCount = skippedExcludeEventCount;
    }

    public long getSkippedCount() {
        return skippedCount;
    }

    public void setSkippedCount(long skippedCount) {
        this.skippedCount = skippedCount;
    }

    /**
     * Skipped exclude event count increase automatically
     */
    public void autoIncreaseSkippedExcludeEventCount() {
        skippedExcludeEventCount++;
    }

    /**
     * Skipped event count increase automatically
     */
    public void autoIncreaseSkippedCount() {
        skippedCount++;
    }

    @Override
    public String toString() {
        return "{"
                + "timestamp=" + timestamp
                + ",\"extractCount\":" + extractCount
                + ",\"skippedExcludeEventCount\":" + skippedExcludeEventCount
                + ",\"skippedCount\":" + skippedCount
                + ",\"replayedCount\":" + replayedCount
                + ",\"successCount\":" + successCount
                + ",\"failCount\":" + failCount
                + ",\"speed\":" + speed
                + ",\"rest\":" + rest
                + ",\"overallPipe\":" + overallPipe
                + '}';
    }
}
