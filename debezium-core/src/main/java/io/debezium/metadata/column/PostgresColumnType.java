/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package io.debezium.metadata.column;

import java.util.Arrays;
import java.util.List;

/**
 * postgresql column type enum
 *
 * @author jianghongbo
 * @since 2025/2/6
 */
public enum PostgresColumnType {
    PG_BIT("bit"),
    PG_BIT_VARYING("bit varying"),
    PG_VARBIT("varbit"),
    PG_CHARACTER("character"),
    PG_CHARACTER_VARING("character varying"),
    PG_CHAR("char"),
    PG_VARCHAR("varchar"),
    PG_BPCHAR("bpchar"),
    PG_NUMERIC("numeric"),
    PG_DECIMAL("decimal"),
    PG_TIME("time"),
    PG_TIMESTAMP("timestamp"),
    PG_TIME_TZ("timetz"),
    PG_TIMESTAMP_TZ("timestamptz"),
    PG_INTERVAL("interval"),
    PG_SERIAL("serial"),
    PG_SMALLSERIAL("smallserial"),
    PG_BIGSERIAL("bigserial"),
    PG_LINE("line")
    ;
    PostgresColumnType(String code) {
        this.code = code;
    }

    private String code;

    public String code() {
        return code;
    }

    static List<PostgresColumnType> typesWithLength = Arrays.asList(PG_BIT, PG_BIT_VARYING, PG_VARBIT, PG_CHARACTER,
            PG_CHARACTER_VARING, PG_CHAR, PG_VARCHAR, PG_BPCHAR, PG_NUMERIC, PG_DECIMAL, PG_TIMESTAMP, PG_TIME,
            PG_TIME_TZ, PG_TIMESTAMP_TZ, PG_INTERVAL);
    static List<PostgresColumnType> typesNumerics = Arrays.asList(PG_NUMERIC, PG_DECIMAL);

    static List<PostgresColumnType> typesVars = Arrays.asList(PG_VARBIT, PG_VARCHAR, PG_BIT_VARYING,
            PG_CHARACTER_VARING);
    static List<PostgresColumnType> typesTimes = Arrays.asList(PG_TIME, PG_TIMESTAMP, PG_TIME_TZ, PG_TIMESTAMP_TZ);
    static List<PostgresColumnType> typesSerials = Arrays.asList(PG_SERIAL, PG_SMALLSERIAL, PG_BIGSERIAL);

    /**
     * is type with length
     *
     * @param typeName String
     * @return boolean
     */
    public static boolean isTypeWithLength(String typeName) {
        for (PostgresColumnType type : typesWithLength) {
            if (type.code().equals(typeName)) {
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
        for (PostgresColumnType type : typesNumerics) {
            if (type.code().equals(typeName)) {
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
        for (PostgresColumnType type : typesVars) {
            if (type.code().equals(typeName)) {
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
        for (PostgresColumnType type : typesTimes) {
            if (type.code().equals(typeName)) {
                return true;
            }
        }
        return false;
    }

    /**
     * is interval type
     *
     * @param typeName String
     * @return boolean
     */
    public static boolean isTypesInterval(String typeName) {
        return PG_INTERVAL.code().equals(typeName);
    }

    /**
     * is serial type
     *
     * @param typeName String
     * @return boolean
     */
    public static boolean isSerialTypes(String typeName) {
        for (PostgresColumnType type : typesSerials) {
            if (type.code().equals(typeName)) {
                return true;
            }
        }
        return false;
    }

    /**
     * is line type
     *
     * @param typeName String
     * @return boolean
     */
    public static boolean isTypeLine(String typeName) {
        return PG_LINE.code().equals(typeName);
    }
}
