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

package org.full.migration.source.opengauss;

import org.full.migration.constants.OpenGaussConstants;
import org.full.migration.enums.SqlCompatibilityEnum;
import org.full.migration.model.PostgresCustomTypeMeta;
import org.full.migration.model.config.GlobalConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * OpenGaussSource for sql_compatibility B
 *
 * @since 2026-03-13
 */
public class OpenGaussBSource extends OpenGaussSource {
    public OpenGaussBSource(GlobalConfig globalConfig) {
        super(globalConfig, SqlCompatibilityEnum.B);
    }

    @Override
    protected String getDatabaseType() {
        return "opengauss_b";
    }

    @Override
    String getQueryTableSql() {
        return OpenGaussConstants.QUERY_TABLE_SQL_B;
    }

    @Override
    String filterEnumSet(String createTableSql) {
        return createTableSql.replaceAll("::\\w+\\.?\\w+,", ",");
    }

    @Override
    public List<PostgresCustomTypeMeta> queryCustomOrDomainTypes(Set<String> schemaSet) {
        return new ArrayList<>();
    }
}
