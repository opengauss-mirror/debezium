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

package org.full.migration.strategy;

import org.full.migration.object.SourceConfig;
import org.full.migration.object.Table;
import org.full.migration.source.SourceDatabase;
import org.full.migration.target.TargetDatabase;

import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * TableMigration
 *
 * @since 2025-03-15
 */
public class TableMigration extends MigrationStrategy {
    /**
     * Constructor
     *
     * @param sourceDatabase sourceDatabase
     * @param targetDatabase targetDatabase
     */
    public TableMigration(SourceDatabase sourceDatabase, TargetDatabase targetDatabase) {
        super(sourceDatabase, targetDatabase);
    }

    @Override
    public void migration() {
        Set<String> schemaSet = source.getSchemaSet();
        target.createSchemas(schemaSet);
        SourceConfig sourceConfig = source.getSourceConfig();
        int readerCount = sourceConfig.getReaderNum();
        int writeCount = sourceConfig.getWriterNum();
        ExecutorService readExecutor = Executors.newFixedThreadPool(readerCount);
        ExecutorService writeExecutor = Executors.newFixedThreadPool(writeCount);
        Set<Table> tables = source.queryTableSet(schemaSet);
        for (Table table : tables) {
            readExecutor.submit(() -> source.readTable(table, sourceConfig.getDbConn()));
            writeExecutor.submit(() -> target.writeTable(target.getDbConfig()));
        }
        readExecutor.shutdown();
        writeExecutor.shutdown();
    }
}
