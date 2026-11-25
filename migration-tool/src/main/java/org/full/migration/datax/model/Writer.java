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
 * Writer
 * Represents the writer section in DataX configuration.
 * Defines the data target configuration including the writer plugin name
 * and connection parameters.
 *
 * @since 2025-04-18
 */
@Data
public class Writer {
    private String name;
    private WriterParameter parameter;

    /**
     * Default constructor that initializes with "gaussdbwriter" as the default plugin.
     */
    public Writer() {
        this.name = "gaussdbwriter";
        this.parameter = new WriterParameter();
    }

    /**
     * Constructor with specified name and parameter.
     *
     * @param name      The writer plugin name
     * @param parameter The writer parameters
     */
    public Writer(String name, WriterParameter parameter) {
        this.name = name;
        this.parameter = parameter;
    }

    @Override
    public String toString() {
        return "Writer{name='" + name + '\'' + ", parameter=" + parameter + '}';
    }
}