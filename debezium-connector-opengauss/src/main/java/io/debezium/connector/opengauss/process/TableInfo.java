/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.opengauss.process;

/**
 * Description: TableInfo
 *
 * @author czy
 * @since 2023-06-07
 */
public class TableInfo {
    private String name;
    private String schema;
    private int status;
    private float percent;
    private int record;
    private double data;
    private int processRecord;

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
    public int getRecord() {
        return record;
    }

    /**
     * Set
     *
     * @param record record
     */
    public void setRecord(int record) {
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
     * Get
     *
     * @return processRecord
     */
    public int getProcessRecord() {
        return processRecord;
    }

    /**
     * Set
     *
     * @param processRecord processRecord
     */
    public void setProcessRecord(int processRecord) {
        this.processRecord = processRecord;
    }

    /**
     * Get
     *
     * @return schema
     */
    public String getSchema() {
        return schema;
    }

    /**
     * Set
     *
     * @param schema String
     */
    public void setSchema(String schema) {
        this.schema = schema;
    }

    /**
     * update Status
     *
     * @param progressStatus ProgressStatus
     */
    public void updateStatus(ProgressStatus progressStatus) {
        this.status = progressStatus.getCode();
    }
}
