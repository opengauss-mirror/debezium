/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
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
