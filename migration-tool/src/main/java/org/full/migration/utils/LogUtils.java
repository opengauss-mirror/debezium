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

import org.full.migration.exception.MigrationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 日志工具类
 */
public class LogUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(LogUtils.class);
    
    /**
     * 记录异常信息
     */
    public static void logException(MigrationException e) {
        // 使用结构化日志记录
        LOGGER.error("Migration error: [{}] {}, Details: {}, Cause: {}",
                e.getErrorCode(),
                e.getErrorMessage(),
                e.getDetails(),
                e.getCause() != null ? e.getCause().getMessage() : "null",
                e);
    }
    
    /**
     * 记录普通异常信息
     */
    public static void logException(Exception e) {
        LOGGER.error("Unexpected error: {}", e.getMessage(), e);
    }
}