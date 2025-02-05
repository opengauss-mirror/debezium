/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.process;

/**
 * TotalInfo
 *
 * @author jianghongbo
 * @since 2025-02-05
 */
public class TotalInfo {
    private long record;
    private double data;
    private int time;
    private double speed;

    /**
     * Constructor
     *
     * @param record int
     * @param data double
     * @param time int
     * @param speed double
     */
    public TotalInfo(long record, double data, int time, double speed) {
        this.record = record;
        this.data = data;
        this.time = time;
        this.speed = speed;
    }

    public TotalInfo() {
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
     * @param record int
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
     * @param data double
     */
    public void setData(double data) {
        this.data = data;
    }

    /**
     * Get
     *
     * @return time
     */
    public long getTime() {
        return time;
    }

    /**
     * Set
     *
     * @param time long
     */
    public void setTime(int time) {
        this.time = time;
    }

    /**
     * Get
     *
     * @return speed
     */
    public double getSpeed() {
        return speed;
    }

    /**
     * Set
     *
     * @param speed double
     */
    public void setSpeed(double speed) {
        this.speed = speed;
    }
}
