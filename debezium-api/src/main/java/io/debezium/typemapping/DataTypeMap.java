/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.typemapping;

import java.util.HashMap;

/**
 * Description: DataTypeMap class
 * @author douxin
 * @date 2022/11/16
 **/
public class DataTypeMap {
    // mysql data type
    public static final String M_HEX_BLOB = "blob";
    public static final String M_HEX_T_BLOB = "tinyblob";
    public static final String M_HEX_M_BLOB = "mediumblob";
    public static final String M_HEX_L_BLOB = "longblob";
    public static final String M_S_GIS_MUL_POINT = "multipoint";
    public static final String M_S_GIS_MUL_LINESTR = "multilinestring";
    public static final String M_S_GIS_MUL_POLYGON = "multipolygon";
    public static final String M_S_GIS_GEOCOL = "geometrycollection";
    public static final String M_S_GIS_GEOCOL2 = "geomcollection";
    public static final String M_C_GIS_POINT = "point";
    public static final String M_C_GIS_GEO = "geometry";
    public static final String M_C_GIS_LINESTR = "linestring";
    public static final String M_C_GIS_POLYGON = "polygon";
    public static final String M_JSON = "json";
    public static final String M_BINARY = "binary";
    public static final String M_VARBINARY = "varbinary";
    public static final String M_BIT = "bit";
    public static final String M_DATATIME = "datetime";
    public static final String M_TIMESTAMP = "timestamp";
    public static final String M_DATE = "date";
    public static final String M_INTEGER = "integer";
    public static final String M_MINT = "mediumint";
    public static final String M_TINT = "tinyint";
    public static final String M_SINT = "smallint";
    public static final String M_INT = "int";
    public static final String M_BINT = "bigint";
    public static final String M_VARCHAR = "varchar";
    public static final String M_CHAR_VAR = "character varying";
    public static final String M_TEXT = "text";
    public static final String M_CHAR = "char";
    public static final String M_TIME = "time";
    public static final String M_TTEXT = "tinytext";
    public static final String M_MTEXT = "mediumtext";
    public static final String M_LTEXT = "longtext";
    public static final String M_DECIMAL = "decimal";
    public static final String M_DEC = "dec";
    public static final String M_NUM = "numeric";
    public static final String M_DOUBLE = "double";
    public static final String M_DOUBLE_P = "double precision";
    public static final String M_FLOAT = "float";
    public static final String M_FLOAT4 = "float4";
    public static final String M_FLOAT8 = "float8";
    public static final String M_REAL = "real";
    public static final String M_FIXED = "fixed";
    public static final String M_YEAR = "year";
    public static final String M_ENUM = "enum";
    public static final String M_SET = "set";
    public static final String M_BOOL = "bool";
    public static final String M_BOOLEAN = "boolean";

    // openGauss data type
    public static final String O_INTEGER = "integer";
    public static final String O_BINT = "bigint";
    public static final String O_TIMESTAP = "timestamp";
    public static final String O_TIMESTAP_NO_TZ = "timestamp without time zone";
    public static final String O_DATE = "date";
    public static final String O_TIME = "time";
    public static final String O_TIME_NO_TZ = "time without time zone";
    public static final String O_BLOB = "blob";
    public static final String O_BYTEA = "bytea";
    public static final String O_BIT = "bit";
    public static final String O_NUM = "numeric";
    public static final String O_FLOAT = "float";
    public static final String O_BIGSERIAL = "bigserial";
    public static final String O_SERIAL = "serial";
    public static final String O_DOUBLE_P = "double precision";
    public static final String O_DEC = "decimal";
    public static final String O_ENUM = "enum";
    public static final String O_JSON = "json";
    public static final String O_BOOLEAN = "boolean";
    public static final String O_POINT = "point";
    public static final String O_PATH = "path";
    public static final String O_POLYGON = "polygon";
    public static final String O_GEO = "geometry";
    public static final String O_C_BPCHAR = "bpchar";
    public static final String O_C_NCHAR = "nchar";
    public static final String O_C_VARCHAR = "varchar";
    public static final String O_C_VARCHAR2 = "varchar2";
    public static final String O_C_NVCHAR2 = "nvarchar2";
    public static final String O_C_CLOB = "clob";
    public static final String O_C_CHAR = "char";
    public static final String O_C_CHARACTER = "character";
    public static final String O_C_CHAR_VAR = "character varying";
    public static final String O_C_TEXT = "text";
    public static final String O_SET = "set";

    public static HashMap<String, String> mysql2OpenGaussTypeMap = new HashMap<String, String>() {
        {
            put(M_INTEGER, O_INTEGER);
            put(M_INTEGER, O_INTEGER);
            put(M_MINT, O_INTEGER);
            put(M_TINT, O_INTEGER);
            put(M_SINT, O_INTEGER);
            put(M_INT, O_INTEGER);
            put(M_BINT, O_BINT);
            put(M_VARCHAR, O_C_CHAR_VAR);
            put(M_CHAR_VAR, O_C_CHAR_VAR);
            put(M_TEXT, O_C_TEXT);
            put(M_CHAR, O_C_CHARACTER);
            put(M_DATATIME, O_TIMESTAP_NO_TZ);
            put(M_DATE, O_DATE);
            put(M_TIME, O_TIME_NO_TZ);
            put(M_TIMESTAMP, O_TIMESTAP_NO_TZ);
            put(M_TTEXT, O_C_TEXT);
            put(M_MTEXT, O_C_TEXT);
            put(M_LTEXT, O_C_TEXT);
            put(M_HEX_T_BLOB, O_BLOB);
            put(M_HEX_M_BLOB, O_BLOB);
            put(M_HEX_L_BLOB, O_BLOB);
            put(M_HEX_BLOB, O_BLOB);
            put(M_BINARY, O_BYTEA);
            put(M_VARBINARY, O_BYTEA);
            put(M_DECIMAL, O_NUM);
            put(M_DEC, O_NUM);
            put(M_NUM, O_NUM);
            put(M_DOUBLE, O_NUM);
            put(M_DOUBLE_P, O_NUM);
            put(M_FLOAT, O_NUM);
            put(M_FLOAT4, O_NUM);
            put(M_FLOAT8, O_NUM);
            put(M_REAL, O_NUM);
            put(M_FIXED, O_NUM);
            put(M_BIT, O_INTEGER);
            put(M_YEAR, O_INTEGER);
            put(M_ENUM, O_ENUM);
            put(M_SET, O_SET);
            put(M_JSON, O_JSON);
            put(M_BOOL, O_BOOLEAN);
            put(M_BOOLEAN, O_BOOLEAN);
            put(M_C_GIS_GEO, O_GEO);
            put(M_C_GIS_POINT, O_GEO);
            put(M_C_GIS_LINESTR, O_GEO);
            put(M_C_GIS_POLYGON, O_GEO);
            put(M_S_GIS_MUL_POINT, O_GEO);
            put(M_S_GIS_GEOCOL, O_GEO);
            put(M_S_GIS_GEOCOL2, O_GEO);
            put(M_S_GIS_MUL_LINESTR, O_GEO);
            put(M_S_GIS_MUL_POLYGON, O_GEO);
            put(M_C_GIS_GEO, O_POINT);
            put(M_C_GIS_POINT, O_POINT);
            put(M_C_GIS_LINESTR, O_PATH);
            put(M_C_GIS_POLYGON, O_POLYGON);
            put(M_S_GIS_MUL_POINT, O_BYTEA);
            put(M_S_GIS_GEOCOL, O_BYTEA);
            put(M_S_GIS_GEOCOL2, O_BYTEA);
            put(M_S_GIS_MUL_LINESTR, O_BYTEA);
            put(M_S_GIS_MUL_POLYGON, O_BYTEA);
        }
    };
}
