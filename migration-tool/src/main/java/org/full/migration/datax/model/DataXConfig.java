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
 * DataXConfig
 * Represents the complete DataX configuration structure.
 * This is the root class for DataX job configuration, containing the job definition
 * with its settings and content specifications.
 *
 * <p>The configuration structure follows DataX's JSON format:</p>
 * <pre>
 * {
 *   "job": {
 *     "setting": { ... },
 *     "content": [ ... ]
 *   }
 * }
 * </pre>
 *
 * @since 2025-04-18
 */
@Data
public class DataXConfig {
    private Job job;

    /**
     * Default constructor that initializes with an empty Job.
     */
    public DataXConfig() {
        this.job = new Job();
    }

    /**
     * Constructor with a specified Job.
     *
     * @param job The job configuration to use
     */
    public DataXConfig(Job job) {
        this.job = job;
    }

    @Override
    public String toString() {
        return "DataXConfig{job=" + job + '}';
    }
}