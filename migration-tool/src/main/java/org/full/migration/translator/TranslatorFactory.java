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

package org.full.migration.translator;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * TranslatorFactory
 * Translator factory class for managing and providing various database type translators
 *
 * @since 2025-05-20
 */
public class TranslatorFactory {
    private static final Map<String, Source2TargetTranslator> translators = new HashMap<>();

    static {
        // Register supported database types and their translators
        // PostgreSQL to openGauss translator
        translators.put("postgresql2opengauss", new Postgresql2OpenGaussTranslator());
        // SQL Server to openGauss translator
        translators.put("sqlserver2opengauss", new SqlServer2OpenGaussTranslator());
        // openGauss to openGauss translator
        translators.put("opengauss2opengauss", new OpenGauss2OpenGaussTranslator());
        // Oracle to OGRAC translator
        translators.put("oracle2ograc", new Oracle2OgracTranslator());
    }

    /**
     * Get the translator for the specified database type
     * @param dbType Database type (e.g., "postgresql", "sqlserver", "oracle")
     * @return Translator instance for the specified database type, or throws IllegalArgumentException if not supported
     */
    public static Source2TargetTranslator getTranslator(String dbType) {
        Source2TargetTranslator translator = translators.get(dbType.toLowerCase());
        if (translator == null) {
            throw new IllegalArgumentException("Unsupported database type: " + dbType);
        }
        return getTranslator(dbType, "opengauss");
    }

    /**
     * Directly translate SQL (simplified call)
     * @param dbType Database type (e.g., "postgresql", "sqlserver", "oracle")
     * @param sql SQL statement to translate
     * @param isDebug Whether to enable debug mode
     * @param isCaseSensitive Whether column names are case sensitive
     * @return Translated SQL statement, or empty if translation is not supported
     */
    public static Optional<String> translate(String dbType, String sql, boolean isDebug, boolean isCaseSensitive) {
        return getTranslator(dbType).translate(sql, isDebug, isCaseSensitive);
    }
    
    /**
     * Get the translator for the specified source and target database types
     * @param sourceDbType Source database type (e.g., "postgresql", "sqlserver", "oracle")
     * @param targetDbType Target database type (e.g., "opengauss", "ograc")
     * @return Translator instance for the specified source and target database types, or throws IllegalArgumentException if not supported
     */
    public static Source2TargetTranslator getTranslator(String sourceDbType, String targetDbType) {
        // Currently only supports specific source-target database combinations
        // Future extensions can support more combinations
         Source2TargetTranslator translator = translators.get(getTranslatorKey(sourceDbType, targetDbType));
        if (translator == null) {
            throw new IllegalArgumentException("Translator for " + sourceDbType + " does not support target database type: " + targetDbType);
        }
        return translator;
    }

    private static String getTranslatorKey(String sourceDbType, String targetDbType) {
        if(targetDbType == null) {
           return sourceDbType.toLowerCase();
        }
        return sourceDbType.toLowerCase() + "2" + targetDbType.toLowerCase();
    }
}
