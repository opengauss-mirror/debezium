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

import org.full.migration.source.SourceDatabase;
import org.full.migration.target.TargetDatabase;

/**
 * ObjectMigration
 *
 * @since 2025-03-15
 */
public class ObjectMigration extends MigrationStrategy {
    private final String objectType;

    /**
     * ObjectMigration
     *
     * @param source source
     * @param target target
     * @param objectType objectType
     */
    public ObjectMigration(SourceDatabase source, TargetDatabase target, String objectType) {
        super(source, target);
        this.objectType = objectType;
    }

    @Override
    public void migration() {
        source.readObjects(objectType);
        target.writeObjects(objectType);
    }
}
