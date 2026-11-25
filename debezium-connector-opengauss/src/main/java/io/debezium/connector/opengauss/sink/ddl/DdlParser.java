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

package io.debezium.connector.opengauss.sink.ddl;

/**
 * <p>
 *     Parse structured text in json format into executable DDL.
 * </p>
 * <p>
 *     Here is an example for the create statement:
 * </p>
 * <p>
 *     {
 *     <pre>
 *       "fmt": "CREATE %{persistence}s TABLE %{if_not_exists}s %{identity}D %{table_elements}s %{inherits}s
 *       WITH (%{with:, }s)",
 *     </pre>
 *       <pre>
 *       "with": [
 *         {
 *           "fmt": "%{label}s = %{value}L",
 *           "label": {
 *             "fmt": "%{label}I",
 *             "label": "compression"
 *           },
 *           "value": "no"
 *         }
 *       ],
 *       </pre>
 *       <pre>
 *       "myowner": "postgres",
 *       "identity": {
 *         "objname": "t2",
 *         "schemaname": "public"
 *       },
 *       "inherits": {
 *         "fmt": "INHERITS (%{parents:, }D)",
 *         "present": false
 *       },
 *       </pre>
 *       <pre>
 *       "persistence": "",
 *       "if_not_exists": "",
 *       "table_elements": {
 *         "fmt": "(%{elements:, }s)",
 *         "elements": [
 *           {
 *             "fmt": "%{name}I %{coltype}T %{collation}s %{default}s %{generated_column}s",
 *             "name": "id",
 *             "type": "column",
 *             "coltype": {
 *               "typmod": "",
 *               "typarray": false,
 *               "typename": "int4",
 *               "schemaname": "pg_catalog"
 *             },
 *             "default": {
 *               "fmt": "DEFAULT %{default}s",
 *               "present": false
 *             },
 *             "collation": {
 *               "fmt": "COLLATE %{name}D",
 *               "present": false
 *             },
 *             "generated_column": {
 *               "fmt": "GENERATED ALWAYS AS (%{generation_expr}s) STORED",
 *               "present": false
 *             }
 *           },
 *           {
 *             "fmt": "%{name}I %{coltype}T %{collation}s %{default}s %{generated_column}s",
 *             "name": "name",
 *             "type": "column",
 *             "coltype": {
 *               "typmod": "(100)",
 *               "typarray": false,
 *               "typename": "varchar",
 *               "schemaname": "pg_catalog"
 *             },
 *             "default": {
 *               "fmt": "DEFAULT %{default}s",
 *               "present": false
 *             },
 *             "collation": {
 *               "fmt": "COLLATE %{name}D",
 *               "name": {
 *                 "objname": "default",
 *                 "schemaname": "pg_catalog"
 *               }
 *             },
 *             "generated_column": {
 *               "fmt": "GENERATED ALWAYS AS (%{generation_expr}s) STORED",
 *               "present": false
 *             }
 *           },
 *           {
 *             "fmt": "CONSTRAINT %{name}I %{definition}s",
 *             "name": "my_table_pkey",
 *             "type": "constraint",
 *             "contype": "primary key",
 *             "definition": "PRIMARY KEY (id)"
 *           }
 *         ]
 *       }
 *       </pre>
 *     }
 * </p>
 *
 * @author tianbin
 * @since 2024-11-11
 */
public interface DdlParser {
    /**
     * Resolve JSON-formatted string
     *
     * @param jsonValue DDL in JSON format
     * @return Executable ddl statements
     */
    String parse(String jsonValue);

    /**
     * Get identifier
     *
     * @return objName
     */
    String identifier();

    /**
     * Whether it leads to changes on the table structure
     *
     * @return true if the ddl will lead to changes on table structure, false otherwise
     */
    boolean isTableRefreshed();

    /**
     * Get owner of the object
     *
     * @return the owner
     */
    String owner();

    /**
     * Indicate how the value for that particular key should be expanded.
     */
    enum ConversionSpecifier {
        DOTTED_NAME,
        IDENTIFIER,
        NUMBER,
        STRING,
        STRING_LITERAL,
        TYPE_NAME
    }

    interface JsonValueAdjuster {
        /**
         * JsonValue adjuster
         *
         * @param jsonValue jsonValue
         * @return result
         */
        String adjust(String jsonValue);
    }
}
