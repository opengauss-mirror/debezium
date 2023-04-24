/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.opengauss.process;

import io.debezium.connector.process.BaseSourceProcessInfo;

/**
 * Description: OgSourceProcessInfo
 *
 * @author wangzhengyuan
 * @since 2023-03-30
 */
public class OgSourceProcessInfo extends BaseSourceProcessInfo {
    /**
     * SourceProcessInfo the sourceProcessInfo
     */
    public static final OgSourceProcessInfo SOURCE_PROCESS_INFO = new OgSourceProcessInfo();

    private long skippedExcludeCount;

    private OgSourceProcessInfo() {
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
     * set skipped exclude count
     *
     * @param skippedExcludeCount Long the skipped exclude count
     */
    public void setSkippedExcludeCount(long skippedExcludeCount) {
        this.skippedExcludeCount = skippedExcludeCount;
    }
}
