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

package org.full.migration.model.config;

import lombok.Data;

/**
 * DataXConfig
 * DataX配置类
 *
 * @since 2025-04-18
 */
@Data
public class DataXParamConfig {
    private String dataxHome;
    private String readerName;
    private String writerName;
    private Integer channel;
    private Integer errorRecordLimit;
    private Double errorPercentageLimit;
    private Integer readBatchSize;
    private Integer readTimeout;
    private Integer writeBatchSize;
    private Integer writeTimeout;
    private Boolean enableBatchWrite;
    private Boolean enablePrepareStatement;
    private Integer batchWriteSize;
    private Integer retryTimes;
    private Integer retryInterval;
    private Boolean enableKeepDataXTemporaryConfig = false;
    private Boolean enableOutputDataxLogs =false;
}