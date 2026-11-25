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

/**
 * FileUtils
 *
 * @since 2025-11-24
 */
public class DatabaseUtils {

    /**
     * Put double quotation marks around the object name
     *
     * @param name database object name
     *
     * @return project name with double quotation
     */
    public static String formatObjName(String name) {
        if (name == null) {
            return null;
        }
        // Always treat the input as a single identifier segment: escape every
        // internal double quote and wrap with outer quotes. Pre-quoted input is
        // never trusted as-is, otherwise an attacker-controlled name that starts
        // with '"' could close the identifier and inject extra SQL.
        return "\"" + name.replace("\"", "\"\"") + "\"";
    }

    /**
     * Put double quotation marks around the column list
     *
     * @param columnNames String the column names
     *
     * @return String the column names with double quotation marks
     */
    public static String formatMultiColName(String columnNames) {
        if (columnNames == null || columnNames.isEmpty()) {
            return columnNames;
        }
        String[] columnList = columnNames.split(",");
        StringBuilder builder = new StringBuilder();
        for (String column : columnList) {
            builder.append(formatObjName(column.trim())).append(",");
        }
        builder.deleteCharAt(builder.length() - 1);
        return builder.toString();
    }
}
