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

import com.alibaba.druid.sql.SQLTransformUtils;
import com.alibaba.druid.sql.ast.SQLDataType;
import com.alibaba.druid.sql.ast.SQLDataTypeImpl;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLObject;
import com.alibaba.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.druid.sql.ast.statement.SQLCharacterDataType;
import com.alibaba.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.druid.util.FnvHash;

import org.apache.commons.math3.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * OpenGaussToSqlserverDataTypeTransformUtil
 *
 * @since 2025-04-18
 */
public class OpenGaussToSqlserverDataTypeTransformUtil extends SQLTransformUtils {
    private static final Logger logger = LoggerFactory.getLogger(OpenGaussToSqlserverDataTypeTransformUtil.class);

    private static final Map<String, Pair<T, String>> typeMap = new HashMap<>() {
        {
            put("TINYINT", new Pair<>(OpenGaussToSqlserverDataTypeTransformUtil.T.NEW, "TINYINT"));
            put("SMALLINT", new Pair<>(OpenGaussToSqlserverDataTypeTransformUtil.T.NEW, "SMALLINT"));
            put("INT", new Pair<>(OpenGaussToSqlserverDataTypeTransformUtil.T.NEW, "INTEGER"));
            put("BIGINT", new Pair<>(OpenGaussToSqlserverDataTypeTransformUtil.T.NEW, "BIGINT"));
            put("BIT", new Pair<>(OpenGaussToSqlserverDataTypeTransformUtil.T.CLONE, "BOOLEAN"));
            put("DECIMAL", new Pair<>(OpenGaussToSqlserverDataTypeTransformUtil.T.CLONE, "DECIMAL"));
            put("NUMERIC", new Pair<>(OpenGaussToSqlserverDataTypeTransformUtil.T.CLONE, "NUMERIC"));
            put("FLOAT", new Pair<>(OpenGaussToSqlserverDataTypeTransformUtil.T.CLONE, "FLOAT"));
            put("REAL", new Pair<>(OpenGaussToSqlserverDataTypeTransformUtil.T.CLONE, "REAL"));
            put("MONEY", new Pair<>(OpenGaussToSqlserverDataTypeTransformUtil.T.NEW, "MONEY"));
            put("SMALLMONEY", new Pair<>(OpenGaussToSqlserverDataTypeTransformUtil.T.CLONE, "DECIMAL"));
            put("CHAR", new Pair<>(OpenGaussToSqlserverDataTypeTransformUtil.T.CLONE, "CHAR"));
            put("VARCHAR", new Pair<>(OpenGaussToSqlserverDataTypeTransformUtil.T.CLONE, "VARCHAR"));
            put("TEXT", new Pair<>(OpenGaussToSqlserverDataTypeTransformUtil.T.NEW, "TEXT"));
            put("NCHAR", new Pair<>(OpenGaussToSqlserverDataTypeTransformUtil.T.NEW, "CHAR"));
            put("NVARCHAR", new Pair<>(OpenGaussToSqlserverDataTypeTransformUtil.T.CLONE, "NVARCHAR"));
            put("NTEXT", new Pair<>(OpenGaussToSqlserverDataTypeTransformUtil.T.NEW, "TEXT"));
            put("BINARY", new Pair<>(OpenGaussToSqlserverDataTypeTransformUtil.T.NEW, "BYTEA"));
            put("VARBINARY", new Pair<>(OpenGaussToSqlserverDataTypeTransformUtil.T.NEW, "BYTEA"));
            put("IMAGE", new Pair<>(OpenGaussToSqlserverDataTypeTransformUtil.T.NEW, "BYTEA"));
            put("DATE", new Pair<>(OpenGaussToSqlserverDataTypeTransformUtil.T.NEW, "DATE"));
            put("TIME", new Pair<>(OpenGaussToSqlserverDataTypeTransformUtil.T.CLONE, "TIME"));
            put("DATETIME", new Pair<>(OpenGaussToSqlserverDataTypeTransformUtil.T.CLONE, "TIMESTAMP"));
            put("DATETIME2", new Pair<>(OpenGaussToSqlserverDataTypeTransformUtil.T.CLONE, "TIMESTAMP"));
            put("DATETIMEOFFSET", new Pair<>(OpenGaussToSqlserverDataTypeTransformUtil.T.NEW, "TIMESTAMPTZ"));
            put("SMALLDATETIME", new Pair<>(OpenGaussToSqlserverDataTypeTransformUtil.T.CLONE, "TIMESTAMP"));
            put("TIMESTAMP", new Pair<>(OpenGaussToSqlserverDataTypeTransformUtil.T.NEW, "BYTEA"));
            put("UNIQUEIDENTIFIER", new Pair<>(OpenGaussToSqlserverDataTypeTransformUtil.T.NEW, "UUID"));
            put("XML", new Pair<>(OpenGaussToSqlserverDataTypeTransformUtil.T.NEW, "XML"));
            put("GEOMETRY", new Pair<>(OpenGaussToSqlserverDataTypeTransformUtil.T.NEW, "GEOMETRY"));
            put("GEOGRAPHY", new Pair<>(OpenGaussToSqlserverDataTypeTransformUtil.T.NEW, "GEOGRAPHY"));
            put("HIERARCHYID", new Pair<>(OpenGaussToSqlserverDataTypeTransformUtil.T.NONSUPPORT, ""));
            put("SQL_VARIANT", new Pair<>(OpenGaussToSqlserverDataTypeTransformUtil.T.NONSUPPORT, ""));
            put("TABLE", new Pair<>(OpenGaussToSqlserverDataTypeTransformUtil.T.NONSUPPORT, ""));
            put("CURSOR", new Pair<>(OpenGaussToSqlserverDataTypeTransformUtil.T.NONSUPPORT, ""));
        }
    };

    enum T {
        NEW,
        CLONE,
        NONSUPPORT;
    }

    /**
     * transformOpenGaussToSqlServer
     *
     * @param type type
     * @return SQLDataType
     */
    public static SQLDataType transformOpenGaussToSqlServer(SQLDataType type) {
        String uName = type.getName().toUpperCase();
        long nameHash = type.nameHashCode64();
        List<SQLExpr> arguments = type.getArguments();
        SQLDataType dataType;
        SQLExpr arg0;
        int precision = -1;
        SQLObject parent = type.getParent();
        boolean isAutoIncrement = parent instanceof SQLColumnDefinition
            && ((SQLColumnDefinition) parent).isAutoIncrement();
        if (typeMap.containsKey(uName)) {
            if (type.isInt() && isAutoIncrement) {
                dataType = new SQLDataTypeImpl("BIGSERIAL");
            } else if (isFloatPoint(nameHash) && arguments.size() > 0) {
                arg0 = arguments.get(0);
                precision = ((SQLIntegerExpr) arg0).getNumber().intValue();
                dataType = new SQLCharacterDataType("FLOAT", precision);
            } else {
                Pair<OpenGaussToSqlserverDataTypeTransformUtil.T, String> p = typeMap.get(uName);
                if (p.getKey() == OpenGaussToSqlserverDataTypeTransformUtil.T.NEW) {
                    dataType = new SQLDataTypeImpl(p.getValue());
                } else if (p.getKey() == OpenGaussToSqlserverDataTypeTransformUtil.T.CLONE) {
                    dataType = type.clone(); // Clone is to retain the content of parentheses
                    dataType.setName(p.getValue());
                } else {
                    logger.error("NOT SUPPORT TYPE " + type.getName());
                    dataType = type.clone();
                }
            }
            if (isAutoIncrement && isFloatPoint(nameHash)) {
                logger.warn("openGauss only support int autoincrement");
            }
        } else {
            logger.error("UNKNOWN TYPE:" + uName);
            dataType = type.clone();
        }
        dataType.setParent(type.getParent());
        return dataType;
    }

    private static boolean isFloatPoint(long nameHash) {
        return nameHash == FnvHash.Constants.FLOAT || nameHash == FnvHash.Constants.REAL;
    }
}
