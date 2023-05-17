/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.opengauss.process;

import io.debezium.connector.process.BaseSinkProcessInfo;

/**
 * Description: OgSinkProcessInfo
 *
 * @author wangzhengyuan
 * @since 2023-03-30
 */
public class OgSinkProcessInfo extends BaseSinkProcessInfo {
    /**
     * SinkProcessInfo the sinkProcessInfo
     */
    public static final OgSinkProcessInfo SINK_PROCESS_INFO = new OgSinkProcessInfo();

    private OgSinkProcessInfo() {
    }

    /**
     * set overall pipe
     *
     * @param createCount Long the overall pipe
     */
    public void setOverallPipe(long createCount) {
        this.overallPipe = createCount - getReplayedCount();
    }
}
