/*
 * Copyright 2026 Huawei Technologies Co., Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
