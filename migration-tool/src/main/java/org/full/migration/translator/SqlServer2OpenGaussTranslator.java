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

package org.full.migration.translator;

import com.alibaba.druid.DbType;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;

import org.full.migration.model.table.Column;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;

/**
 * SqlServer2OpenGaussTranslator
 *
 * @since 2025-04-18
 */
public class SqlServer2OpenGaussTranslator extends Source2OpenGaussTranslator {
    private static final Logger LOGGER = LoggerFactory.getLogger(SqlServer2OpenGaussTranslator.class);

    /**
     * translateSQLServer2openGauss
     *
     * @param sqlIn sqlIn
     * @param isDebug debug
     * @param isColumnCaseSensitive isColumnCaseSensitive
     * @return Optional<String>
     */
    public static Optional<String> translateSQLServer2openGauss(String sqlIn, boolean isDebug,
        boolean isColumnCaseSensitive) {
        final StringBuilder appender = new StringBuilder();
        final SQLServerToOpenGaussOutputVisitor visitor = new SQLServerToOpenGaussOutputVisitor(appender,
            isColumnCaseSensitive);
        List<SQLStatement> sqlStatements;
        try {
            sqlStatements = SQLUtils.parseStatements(sqlIn, DbType.sqlserver);
        } catch (Exception e) {
            return Optional.empty();
        }
        final StringBuilder res = new StringBuilder();
        for (SQLStatement statement : sqlStatements) {
            statement.accept(visitor);
            visitor.println();
            if (isDebug) {
                LOGGER.debug("appender is:{}", appender);
            } else {
                res.append(appender);
            }
            appender.delete(0, appender.length());
        }
        return Optional.of(res.toString());
    }

    @Override
    public Optional<String> translate(String sqlIn, boolean isDebug, boolean isColumnCaseSensitiv) {
        return translateSQLServer2openGauss(sqlIn, isDebug, isColumnCaseSensitiv);
    }

    @Override
    public Optional<String> translateColumnType(String tableName, Column column) {
        return Optional.empty();
    }

    @Override
    public String getSourceDatabaseType() {
        return "sqlserver";
    }
}
