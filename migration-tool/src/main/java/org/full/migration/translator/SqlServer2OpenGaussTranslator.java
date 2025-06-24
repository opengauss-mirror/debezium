/*
 * Copyright (c) 2025-2025 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *           http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package org.full.migration.translator;

import com.alibaba.druid.DbType;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;

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
}
