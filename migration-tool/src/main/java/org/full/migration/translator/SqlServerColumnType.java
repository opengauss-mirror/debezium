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
    SS_NVARCHAR_MAX("nvarchar(max)", "text"),
    SS_TEXT("text", "text"),
    SS_NTEXT("ntext", "text"),
    SS_BINARY("binary", "bytea"),
    SS_VARBINARY("varbinary", "bytea"),
    SS_VARBINARY_MAX("varbinary(max)", "bytea"),
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
    SS_IMAGE("image", "bytea"),
    SS_MONEY("money", "money"),
    SS_SMALLMONEY("smallmoney", "money"),
    SS_ROWVERSION("rowversion", "bytea"),
    SS_TIMESTAMP("timestamp", "bytea"),
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
    static final Set<SqlServerColumnType> GEOMETRY_TYPE_SET = Set.of(SS_GEOMETRY, SS_GEOGRAPHY);
    static final Set<SqlServerColumnType> MONEY_TYPE_SET = Set.of(SS_MONEY, SS_SMALLMONEY);

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
     * isGeometryTypes
     *
     * @param typeName typeName
     * @return isBinaryTypes
     */
    public static boolean isGeometryTypes(String typeName) {
        for (SqlServerColumnType type : GEOMETRY_TYPE_SET) {
            if (type.getSsType().equalsIgnoreCase(typeName)) {
                return true;
            }
        }
        return false;
    }

    /**
     * isMoneyTypes
     *
     * @param typeName typeName
     * @return isBinaryTypes
     */
    public static boolean isMoneyTypes(String typeName) {
        for (SqlServerColumnType type : MONEY_TYPE_SET) {
            if (type.getSsType().equalsIgnoreCase(typeName)) {
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
        return ssType;
    }
}
