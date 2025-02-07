/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package io.debezium.connector.postgresql.sink.utils;

import org.apache.kafka.connect.data.Struct;

/**
 * Description: ValueConverter interface
 *
 * @author tianbin
 * @since 2024-11-25
 **/
interface ValueConverter {
    /**
     * Convert a struct value to a string based on the column name.
     *
     * @param columnName the column name
     * @param value the struct value
     * @return the converted string
     */
    String convert(String columnName, Struct value);
}
