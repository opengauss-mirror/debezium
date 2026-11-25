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
 * Reader
 * Represents the reader section in DataX configuration.
 * Defines the data source configuration including the reader plugin name
 * and connection parameters.
 *
 * @since 2025-04-18
 */
@Data
public class Reader {
    private String name;
    private ReaderParameter parameter;

    /**
     * Default constructor that initializes with "oraclereader" as the default plugin.
     */
    public Reader() {
        this.name = "oraclereader";
        this.parameter = new ReaderParameter();
    }

    /**
     * Constructor with specified name and parameter.
     *
     * @param name      The reader plugin name
     * @param parameter The reader parameters
     */
    public Reader(String name, ReaderParameter parameter) {
        this.name = name;
        this.parameter = parameter;
    }

    @Override
    public String toString() {
        return "Reader{name='" + name + '\'' + ", parameter=" + parameter + '}';
    }
}