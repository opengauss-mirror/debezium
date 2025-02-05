/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package io.debezium.connector.postgresql.process;

/**
 * Table Progress Info
 *
 * @author jianghongbo
 * @since 2025-02-05
 */
public class ProgressInfo {
    private String name;
    private int status = ProgressStatus.NOT_MIGRATED.getCode();
    private float percent;
    private String error;
    private long record;
    private double data;

    /**
     * Get
     *
     * @return name
     */
    public String getName() {
        return name;
    }

    /**
     * Set
     *
     * @param name name
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * Get
     *
     * @return status
     */
    public int getStatus() {
        return status;
    }

    /**
     * Set
     *
     * @param status status
     */
    public void setStatus(int status) {
        this.status = status;
    }

    /**
     * Get
     *
     * @return percent
     */
    public float getPercent() {
        return percent;
    }

    /**
     * Set
     *
     * @param percent percent
     */
    public void setPercent(float percent) {
        this.percent = percent;
    }

    /**
     * Get
     *
     * @return record
     */
    public long getRecord() {
        return record;
    }

    /**
     * Set
     *
     * @param record record
     */
    public void setRecord(long record) {
        this.record = record;
    }

    /**
     * Get
     *
     * @return data
     */
    public double getData() {
        return data;
    }

    /**
     * Set
     *
     * @param data data
     */
    public void setData(double data) {
        this.data = data;
    }

    /**
     * update Status
     *
     * @param progressStatus ProgressStatus
     */
    public void updateStatus(ProgressStatus progressStatus) {
        this.status = progressStatus.getCode();
    }

    public void setError(String error) {
        this.error = error;
    }

    public String getError() {
        return error;
    }
}
