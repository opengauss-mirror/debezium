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

package org.full.migration.datax.model;

import lombok.Data;

/**
 * ErrorLimit
 * Represents the errorLimit section in DataX configuration.
 * Defines the maximum allowed errors before a job is considered failed.
 *
 * @since 2025-04-18
 */
@Data
public class ErrorLimit {
    private int record;
    private double percentage;

    /**
     * Default constructor that initializes with 0 record limit and 0.02 (2%) percentage limit.
     */
    public ErrorLimit() {
        this.record = 0;
        this.percentage = 0.02;
    }

    /**
     * Constructor with specified error limits.
     *
     * @param record     Maximum number of error records allowed
     * @param percentage Maximum error percentage allowed (0.0 to 1.0)
     */
    public ErrorLimit(int record, double percentage) {
        this.record = record;
        this.percentage = percentage;
    }

    @Override
    public String toString() {
        return "ErrorLimit{record=" + record + ", percentage=" + percentage + '}';
    }
}