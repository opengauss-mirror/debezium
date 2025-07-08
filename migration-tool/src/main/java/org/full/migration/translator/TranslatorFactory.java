/*
 * Copyright (c) 2025-2025 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *           http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package org.full.migration.translator;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * StrategyFactory
 *
 * @since 2025-05-20
 */
public class TranslatorFactory {
    private static final Map<String, Source2OpenGaussTranslator> translators = new HashMap<>();

    static {
        // 注册支持的数据库类型及其翻译器
        translators.put("postgresql", new Postgresql2OpenGaussTranslator());
        translators.put("sqlserver", new SqlServer2OpenGaussTranslator());
        translators.put("opengauss", new OpenGauss2OpenGaussTranslator());
    }

    /**
     * 获取对应的SQL翻译器
     * @param dbType 数据库类型（如 "postgresql"、"sqlserver"）
     * @return 翻译器实例，如果类型不支持则抛出异常
     */
    public static Source2OpenGaussTranslator getTranslator(String dbType) {
        Source2OpenGaussTranslator translator = translators.get(dbType.toLowerCase());
        if (translator == null) {
            throw new IllegalArgumentException("Unsupported database type: " + dbType);
        }
        return translator;
    }

    /**
     * 直接翻译SQL（简化调用）
     */
    public static Optional<String> translate(String dbType, String sql, boolean isDebug, boolean isCaseSensitive) {
        return getTranslator(dbType).translate(sql, isDebug, isCaseSensitive);
    }
}
