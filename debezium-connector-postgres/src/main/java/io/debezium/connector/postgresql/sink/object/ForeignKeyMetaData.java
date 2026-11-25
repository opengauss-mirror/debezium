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

package io.debezium.connector.postgresql.sink.object;

/**
 * Description: ForeignKeyMetaData
 *
 * @author tianbin
 * @since 2025-01-14
 */
public class ForeignKeyMetaData {
    private final String schema;
    private final String tableName;
    private final String fkName;

    public ForeignKeyMetaData(String schema, String tableName, String fkName) {
        this.schema = schema;
        this.tableName = tableName;
        this.fkName = fkName;
    }

    public String getSchema() {
        return schema;
    }

    public String getTableName() {
        return tableName;
    }

    public String getFkName() {
        return fkName;
    }
}
