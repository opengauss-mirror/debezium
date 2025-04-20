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

import lombok.Getter;

import java.util.Set;

/**
 * SqlServerColumnType
 *
 * @since 2025-04-18
 */
@Getter
public enum SqlServerColumnType {
    SS_TINYINT("tinyint", "tinyint"),
    SS_SMALLINT("smallint", "smallint"),
    SS_INT("int", "integer"),
    SS_BIGINT("bigint", "bigint"),
    SS_BIT("bit", "boolean"),
    SS_DECIMAL("decimal", "decimal"),
    SS_NUMERIC("numeric", "numeric"),
    SS_FLOAT("float", "float"),
    SS_REAL("real", "real"),
    SS_CHAR("char", "char"),
    SS_VARCHAR("varchar", "varchar"),
    SS_VARCHAR_MAX("varchar(max)", "text"),
    SS_NCHAR("nchar", "nchar"),
    SS_NVARCHAR("nvarchar", "nvarchar"),
    SS_NVARCHAR_MAX("nvarchar(max)", "text"), // NVARCHAR(MAX)，替代TEXT
    SS_TEXT("text", "text"),
    SS_NTEXT("ntext", "text"),
    SS_BINARY("binary", "bytea"),
    SS_VARBINARY("varbinary", "bytea"),
    SS_VARBINARY_MAX("varbinary(max)", "blob"), // VARBINARY(MAX)(""),替代image
    SS_DATE("date", "date"),
    SS_TIME("time", "time"),
    SS_DATETIME("datetime", "timestamp"),
    SS_DATETIME2("datetime2", "timestamp"),
    SS_SMALLDATETIME("smalldatetime", "timestamp"),
    SS_DATETIMEOFFSET("datetimeoffset", "timestamp"),
    SS_UNIQUEIDENTIFIER("uniqueidentifier", "uuid"),
    SS_XML("xml", "xml"),
    SS_GEOMETRY("geometry", "point"),
    SS_GEOGRAPHY("geography", "point"),
    SS_IMAGE("image", "blob"),
    SS_MONEY("money", "money"),
    SS_SMALLMONEY("smallmoney", "money"),
    SS_ROWVERSION("rowversion", "timestamp"),
    SS_TIMESTAMP("timestamp", "timestamp"),
    SS_HIERARCHYID("hierarchyid", "nvarchar(4000)"),
    SS_JSON("json", "jsonb");

    private final String ssType;
    private final String ogType;

    SqlServerColumnType(String ssType, String ogType) {
        this.ssType = ssType;
        this.ogType = ogType;
    }

    static final Set<SqlServerColumnType> LENGTH_TYPE_SET = Set.of(SS_CHAR, SS_VARCHAR, SS_NCHAR, SS_NVARCHAR,
        SS_DECIMAL, SS_NUMERIC, SS_TIME, SS_DATETIME2, SS_DATETIMEOFFSET, SS_FLOAT, SS_VARBINARY);
    static final Set<SqlServerColumnType> NUMERICS_SET = Set.of(SS_DECIMAL, SS_NUMERIC);
    static final Set<SqlServerColumnType> VAR_TYPE_SET = Set.of(SS_VARCHAR, SS_NVARCHAR);
    static final Set<SqlServerColumnType> BINARY_TYPE_SET = Set.of(SS_VARBINARY);
    static final Set<SqlServerColumnType> TIME_TYPE_SET = Set.of(SS_TIME, SS_DATETIME2, SS_DATETIMEOFFSET);

    /**
     * is type with length
     *
     * @param typeName String
     * @return boolean
     */
    public static boolean isTypeWithLength(String typeName) {
        for (SqlServerColumnType type : LENGTH_TYPE_SET) {
            if (type.getSsType().equals(typeName)) {
                return true;
            }
        }
        return false;
    }

    /**
     * is numeric types
     *
     * @param typeName String
     * @return boolean
     */
    public static boolean isNumericType(String typeName) {
        for (SqlServerColumnType type : NUMERICS_SET) {
            if (type.getSsType().equals(typeName)) {
                return true;
            }
        }
        return false;
    }

    /**
     * is varying types
     *
     * @param typeName String
     * @return boolean
     */
    public static boolean isVarsTypes(String typeName) {
        for (SqlServerColumnType type : VAR_TYPE_SET) {
            if (type.getSsType().equals(typeName)) {
                return true;
            }
        }
        return false;
    }

    /**
     * is times types
     *
     * @param typeName String
     * @return boolean
     */
    public static boolean isTimesTypes(String typeName) {
        for (SqlServerColumnType type : TIME_TYPE_SET) {
            if (type.getSsType().equals(typeName)) {
                return true;
            }
        }
        return false;
    }

    /**
     * isBinaryTypes
     *
     * @param typeName typeName
     * @return isBinaryTypes
     */
    public static boolean isBinaryTypes(String typeName) {
        for (SqlServerColumnType type : BINARY_TYPE_SET) {
            if (type.getSsType().equals(typeName)) {
                return true;
            }
        }
        return false;
    }

    /**
     * convertType
     *
     * @param ssType ssType
     * @return ogType
     */
    public static String convertType(String ssType) {
        for (SqlServerColumnType type : SqlServerColumnType.values()) {
            if (type.getSsType().equals(ssType)) {
                return type.ogType;
            }
        }
        return "unknown";
    }
}
