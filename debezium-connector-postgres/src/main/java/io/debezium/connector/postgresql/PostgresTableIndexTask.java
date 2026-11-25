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

package io.debezium.connector.postgresql;

import io.debezium.connector.postgresql.param.PostgresDataEventsParam;

import java.util.List;

/**
 * Description: Postgresql tableIndex info
 *
 * @author jianghongbo
 * @since 2025/02/05
 */
public class PostgresTableIndexTask {
    private final PostgresDataEventsParam postgresDataEventsParam;
    private final List<String> tableIdxDdls;

    public PostgresTableIndexTask(PostgresDataEventsParam postgresDataEventsParam, List<String> tableIdxDdls) {
        this.postgresDataEventsParam = postgresDataEventsParam;
        this.tableIdxDdls = tableIdxDdls;
    }

    public PostgresDataEventsParam getPostgresDataEventsParam() {
        return postgresDataEventsParam;
    }

    public List<String> getTableIdxDdls() {
        return tableIdxDdls;
    }
}
