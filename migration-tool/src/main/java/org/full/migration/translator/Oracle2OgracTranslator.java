/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */

package org.full.migration.translator;

import java.util.Optional;

import org.full.migration.exception.ErrorCode;
import org.full.migration.exception.TranslatorException;
import org.full.migration.model.table.Column;

/**
 * Oracle2OgracTranslator
 * Oracle到OGRAC的SQL转换器
 *
 * @since 2025-06-06
 */
public class Oracle2OgracTranslator implements Source2TargetTranslator {
    @Override
    public String getSourceDatabaseType() {
        return "oracle";
    }

    @Override
    public String getTargetDatabaseType() {
        return "ograc";
    }

    @Override
    public Optional<String> translate(String sqlIn, boolean isDebug, boolean isColumnCaseSensitive) {
        return Optional.of(sqlIn);
    }

    @Override
    public Optional<String> translateColumnType(String tableName, Column column) throws TranslatorException {
        String typeName = column.getTypeName();
        if (typeName.startsWith("TIMESTAMP")) {
            return translateTimestampType(column);
        } else if (typeName.startsWith("INTERVAL")) {
            return translateIntervalType(column);
        }
        return switch (typeName) {
            case "RAW", "LONG RAW" -> translateBinaryType(typeName, column);
            case "CHAR", "VARCHAR2", "NCHAR", "NVARCHAR2" -> translateStringType(typeName, column);
            case "NUMBER", "FLOAT" -> translateNumberType(typeName, column);
            case "DATE", "BLOB", "CLOB", "BINARY_FLOAT", "BINARY_DOUBLE", "DOUBLE PRECISION" -> Optional.of(typeName);
            case "NCLOB" -> Optional.of("CLOB");
            case "NBLOB" -> Optional.of("BLOB");
            case "JSON","XMLTYPE" -> Optional.of("CLOB");
            case "ANYDATA", "BFILE","UROWID" -> {
                throw new TranslatorException(ErrorCode.SQL_TRANSLATION_FAILED.getCode(),
                        tableName + "." + column.getName() + " " + typeName + " is not supported by OGRAC");
            }
            default -> Optional.of(typeName);
        };
    }
    
    /**
     * translateTimestampType Translate TIMESTAMP type to OGRAC compatible format
     * Oracle TIMESTAMP(n>6) is not supported by OGRAC, When scale > 6, OGRAC doesn't support it,
     * need to convert to TIMESTAMP(6) with precision loss
     * TIMESTAMP,TIMESTAMP(n),TIMESTAMP(n) WITH LOCAL TIME ZONE,TIMESTAMP(n) WITH TIME ZONE
     * @param column column info
     * @return TIMESTAMP type name in OGRAC compatible format
     */
    private Optional<String> translateTimestampType(Column column) {
        String typeName = column.getTypeName();
        if (column.getScale() != null && column.getScale() > 6) {
            String baseTypeName = typeName.replaceAll("\\(\\d+\\)", "").trim();
            return Optional.of(baseTypeName.replace("TIMESTAMP", "TIMESTAMP(6)"));
        } else {
            return Optional.of(typeName);
        }
    }
    
    /**
     * Translate INTERVAL type to OGRAC compatible format
     * Oracle INTERVAL YEAR(n>4) is not supported by OGRAC, When length > 4, OGRAC doesn't support it,
     * need to convert to INTERVAL YEAR(4) with precision loss
     * @param column column info
     * @return INTERVAL type name in OGRAC compatible format
     */
    private Optional<String> translateIntervalType(Column column) {
        String typeName = column.getTypeName();
        if (typeName.startsWith("INTERVAL YEAR")) {
            long length = column.getLength() > 4 ? 4 : column.getLength();
            String tempIntervalYearToMonth = "INTERVAL YEAR(%d) TO MONTH";
            return Optional.of(String.format(tempIntervalYearToMonth, length));
        } else if (typeName.startsWith("INTERVAL DAY")) {
            int scale = column.getScale() > 6 ? 6 : column.getScale();
            long length = column.getLength() > 6 ? 6 : column.getLength();
            String tempIntervalToSecond = "INTERVAL DAY(%d) TO SECOND(%d)";
            return Optional.of(String.format(tempIntervalToSecond, length, scale));
        }
        return Optional.of(typeName);
    }
    
    /**
     * Translate binary type to OGRAC compatible format
     * @param typeName type name
     * @param column column info
     * @return type name in OGRAC compatible format
     */
    private Optional<String> translateBinaryType(String typeName, Column column) {
        if (typeName.equals("LONG RAW")) {
            return Optional.of("BLOB");
        } else if (typeName.equals("RAW")) {
            return Optional.of(typeName + "(" + column.getLength() + ")");
        }
        return Optional.of(typeName);
    }
    
    /**
     * Translate string type to OGRAC compatible format
     * @param typeName type name
     * @param column column info
     * @return type name in OGRAC compatible format
     */
    private Optional<String> translateStringType(String typeName, Column column) {
        StringBuilder typeBuilder = new StringBuilder(typeName);
        typeBuilder.append("(").append(column.getLength());
        if ((typeName.equals("CHAR") || typeName.equals("VARCHAR2")) && 
            column.getCharUsed() != null && !column.getCharUsed().isEmpty()) {
            typeBuilder.append(" ").append(column.getCharUsed());
        }
        typeBuilder.append(")");
        return Optional.of(typeBuilder.toString());
    }
    
    /**
     * Translate number type to OGRAC compatible format
     * length ==0 ,convert to number
     * scale ==0 ,convert to number(length)
     * @param typeName type name
     * @param column column info
     * @return type name in OGRAC compatible format
     */
    private Optional<String> translateNumberType(String typeName, Column column) {
        if (typeName.equals("NUMBER")) {
            if(column.getLength()==0) {
                return Optional.of(typeName);
            }
            if (column.getScale() == null || column.getScale() == 0) {
                return Optional.of(typeName+"("+column.getLength()+")");
            }else {
                return Optional.of(typeName+"("+column.getLength()+","+column.getScale()+")");
            }
        } else if (typeName.equals("FLOAT")) {
            return Optional.of("DECIMAL(38," + column.getLength() + ")");
        }
        return Optional.of(typeName);
    }

    /**
     * oracle indexType
     * NORMAL
     * IOT - TOP
     * DOMAIN
     * FUNCTION-BASED BITMAP
     * FUNCTION-BASED NORMAL
     * LOB
     * BITMAP
     * @throws TranslatorException 
     */
    @Override
    public Optional<String> translateIndex(String indexType, boolean isDebug) throws TranslatorException {
        return Optional.of("");
    }

    @Override
    public Optional<String> translateFunction(String functionCall, boolean isDebug) {
        String translatedFunction = translateToDateFunction(functionCall);
        return Optional.of(translatedFunction);
    }
    
    /**
     * translateToDateFunction
     * Oracle TO_DATE(char [, fmt [, 'nlsparam']]) -> Ograc to_date(char, fmt)
     * mode 3 is supported, ignore the third parameter nlsparam
     * to_date('2025-03-28', 'YYYY-MM-DD', 'NLS_DATE_LANGUAGE=AMERICAN')
     * TO_DATE(' 2023-01-01 00:00:00', 'SYYYY-MM-DD HH24:MI:SS', 'NLS_CALENDAR=GREGORIAN')
     * @param functionCall function call string
     * @return translated function call string
     */
    private String translateToDateFunction(String functionCall) {
        if (functionCall.toLowerCase().contains("to_date(")) {
            String tripleParamPattern = "(?i)to_date\\(\\s*'([^']+)'\\s*,\\s*'([^']+)'\\s*,\\s*'[^']*'\\s*\\)";
            if (functionCall.matches(".*" + tripleParamPattern + ".*")) {
                String charValue = functionCall.replaceAll(".*" + tripleParamPattern + ".*", "$1");
                String oracleFormat = functionCall.replaceAll(".*" + tripleParamPattern + ".*", "$2");
                oracleFormat = oracleFormat.replace("SYYYY", "YYYY");
                return "to_date('" + charValue + "', '" + oracleFormat + "')";
            }
        }
        return functionCall;
    }
}