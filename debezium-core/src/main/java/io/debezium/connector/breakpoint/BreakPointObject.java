/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.breakpoint;

/**
 * Description: break point info
 *
 * @author lvlintao
 * @since 2023/06/06
 */

public class BreakPointObject {
    private Long lsn;
    private String gtid;
    private Long beginOffset;
    private Long endOffset;
    private String timeStamp;

    /**
     * Gets timeStamp.
     *
     * @return the value of timeStamp
     */
    public String getTimeStamp() {
        return timeStamp;
    }

    /**
     * Sets the timeStamp.
     *
     * @param timeStamp the timestamp at this time
     */
    public void setTimeStamp(String timeStamp) {
        this.timeStamp = timeStamp;
    }

    /**
     * Gets lsn.
     *
     * @return the value of lsn
     */
    public Long getLsn() {
        return lsn;
    }

    /**
     * Sets the lsn.
     *
     * @param lsn the value of lsn
     */
    public void setLsn(Long lsn) {
        this.lsn = lsn;
    }

    /**
     * Gets gtid.
     *
     * @return the value of gtid
     */
    public String getGtid() {
        return gtid;
    }

    /**
     * Sets the gtid.
     *
     * @param gtid the value of gtid
     */
    public void setGtid(String gtid) {
        this.gtid = gtid;
    }

    /**
     * Gets beginOffset.
     *
     * @return the value of beginOffset
     */
    public Long getBeginOffset() {
        return beginOffset;
    }

    /**
     * Sets the beginOffset.
     *
     * @param beginOffset the value of beginOffset
     */
    public void setBeginOffset(Long beginOffset) {
        this.beginOffset = beginOffset;
    }

    /**
     * Gets endOffset.
     *
     * @return the value of endOffset
     */
    public Long getEndOffset() {
        return endOffset;
    }

    /**
     * Sets the endOffset.
     *
     * @param endOffset the value of endOffset
     */
    public void setEndOffset(Long endOffset) {
        this.endOffset = endOffset;
    }

    @Override
    public String toString() {
        return "BreakPointObject{"
                + "lsn=" + lsn
                + ", gtid='" + gtid + '\''
                + ", beginOffset=" + beginOffset
                + ", endOffset=" + endOffset
                + ", timeStamp='" + timeStamp + '\''
                + '}';
    }
}
