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

package org.full.migration.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import org.full.migration.datax.model.DataXConfig;
import org.full.migration.exception.DataXMigrationException;
import org.full.migration.exception.ErrorCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DataXJsonUtils
 * JSON serialization and deserialization utility for DataX configuration objects
 *
 * @since 2025-04-18
 */
public class DataXJsonUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(DataXJsonUtils.class);

    /**
     * Serialize DataXConfig object to formatted JSON string
     *
     * @param config DataXConfig object to serialize
     * @return JSON string representation
     * @throws DataXMigrationException DataXMigrationException
     */
    public static String toJson(DataXConfig config) throws DataXMigrationException {
        try {
            return JSON.toJSONString(config, SerializerFeature.PrettyFormat);
        } catch (Exception e) {
            LOGGER.error("Failed to serialize DataXConfig to JSON: {}", e.getMessage());
            throw new DataXMigrationException(ErrorCode.DATAX_CONFIG_ERROR.getCode(), config.toString(), e);
        }
    }
}