/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */

package org.full.migration.translator;

import java.math.BigDecimal;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.full.migration.exception.ErrorCode;
import org.full.migration.exception.TranslatorException;
import org.full.migration.model.table.Column;
import org.full.migration.model.table.SequenceDefinition;

/**
 * Oracle2OgracTranslator
 * Oracle到OGRAC的SQL转换器
 *
 * @since 2025-06-06
 */
public class Oracle2OgracTranslator implements Source2TargetTranslator {
    private static final Pattern EDITIONABLE_PATTERN = Pattern.compile(
            "CREATE\\s+OR\\s+REPLACE\\s+(?:EDITIONABLE|NONEDITIONABLE)\\s+(\\w+)",
            Pattern.CASE_INSENSITIVE);
    private static final Pattern FUNCTION_PATTERN = Pattern.compile(
            "(CREATE OR REPLACE FUNCTION\\s+)\"[A-Za-z0-9#_]+\".\"([A-Za-z0-9_]+)\"",
            Pattern.CASE_INSENSITIVE
    );
    private static final Pattern TRIGGER_PATTERN = Pattern.compile(
            "(CREATE OR REPLACE TRIGGER\\s+)\"[A-Za-z0-9#_]+\".\"([A-Za-z0-9_]+)\"",
            Pattern.CASE_INSENSITIVE
    );
    private static final Pattern ALTER_TRIGGER_PATTERN = Pattern.compile(
            "^\\s*ALTER TRIGGER\\s+\".*?\".*?$",
            Pattern.CASE_INSENSITIVE | Pattern.MULTILINE
    );
    private static final Pattern PROCEDURE_PATTERN = Pattern.compile(
            "(CREATE OR REPLACE PROCEDURE\\s+)\"[A-Za-z0-9#_]+\".\"([A-Za-z0-9_]+)\"",
            Pattern.CASE_INSENSITIVE
    );
    private static final Pattern VIEW_WITH_READ_ONLY_TRANSLATOR_PATTERN = Pattern.compile(
            "(CREATE OR REPLACE VIEW\\s+.+?)\\s+(AS\\s+[\\s\\S]+?)\\s*(WITH READ ONLY)\\s*;?",
            Pattern.CASE_INSENSITIVE
    );

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
     * translateIntervalType Translate INTERVAL type to OGRAC compatible format
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
        if (column.isAutoIncremented()) {
            return Optional.of("BIGINT");
        }
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
    public Optional<String> translatePartitionFunction(String functionCall, boolean isDebug) {
        String translatedFunction = translateToDateFunction(functionCall);
        return Optional.of(translatedFunction);
    }

    @Override
    public Optional<String> translateView(String name, String viewDDL) {
        if (viewDDL == null || viewDDL.isBlank()) {
            return Optional.empty();
        }
        String viewDefinition = "CREATE OR REPLACE VIEW " + name + " AS " + viewDDL;
        Matcher matcher = VIEW_WITH_READ_ONLY_TRANSLATOR_PATTERN.matcher(viewDefinition);
        if (matcher.matches()) {
            // Reorder the syntax order: View clause + WITH READ ONLY + AS + Query body
            String translated = matcher.group(1) + " " + matcher.group(3) + " " + matcher.group(2);
            return Optional.of(translated);
        }
        return Optional.of(viewDefinition);
    }


    @Override
    public Optional<String> translateFunction(String functionDDL) {
        String defSql = removeEditionableKeywords(functionDDL, "FUNCTION");
        return Optional.of(removeObjectSqlOwner(FUNCTION_PATTERN, defSql));
    }

    @Override
    public Optional<String> translateProcedure(String procedureDDL) {
        String procedureDef = removeEditionableKeywords(procedureDDL, "PROCEDURE");
        return Optional.of(removeObjectSqlOwner(PROCEDURE_PATTERN, procedureDef));
    }

    @Override
    public Optional<String> translateTrigger(String triggerDDL) {
        String triggerDef = removeEditionableKeywords(triggerDDL, "TRIGGER");
        triggerDef = removeObjectSqlOwner(TRIGGER_PATTERN, triggerDef);
        return Optional.of(removeAlterTriggerLine(triggerDef));
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

    private static String removeAlterTriggerLine(String ddl) {
        return ALTER_TRIGGER_PATTERN.matcher(ddl).replaceAll("");
    }



    /**
     * remove editionable/noneditionable keywords from object definition
     * 
     * @param definition Object Definition
     * @param objectType Object Type
     * @return Object Definition with EDITIONABLE/NONEDITIONABLE Keywords Removed
     */
    private static String removeEditionableKeywords(String definition, String objectType) {
        if (definition == null || definition.isEmpty() || objectType == null || objectType.isEmpty()) {
            return definition;
        }
        
        Matcher matcher = EDITIONABLE_PATTERN.matcher(definition);
        if (!matcher.find()) {
            return definition;
        }
        
        String matchedType = matcher.group(1);
        if (!matchedType.equalsIgnoreCase(objectType)) {
            return definition;
        }
        
        return matcher.replaceFirst("CREATE OR REPLACE " + objectType);
    }

    /**
     * remove owner from object SQL definition
     * 
     * @param pattern Pattern to match owner
     * @param sql SQL string
     * @return SQL string with owner removed
     */
    private static String removeObjectSqlOwner(Pattern pattern, String sql) {
        return applyPattern(pattern, sql, "$1\"$2\"");
    }

    /**
     * Apply pattern to SQL string with specified replacement
     * 
     * @param pattern Pattern to apply
     * @param sql SQL string
     * @param replacement Replacement string
     * @return Modified SQL string
     */
    private static String applyPattern(Pattern pattern, String sql, String replacement) {
        if (sql == null || sql.isEmpty()) {
            return sql;
        }
        Matcher matcher = pattern.matcher(sql);
        return matcher.replaceAll(replacement);
    }

    @Override
    public Optional<String> translateSequence(SequenceDefinition sequence) {
        return Optional.of(buildSequenceDdl(sequence));
    }
    
    private String buildSequenceDdl(SequenceDefinition sequence) {
        StringBuilder ddl = new StringBuilder();
        
        ddl.append("CREATE SEQUENCE ").append(sequence.getSequenceName());
        ddl.append(" START WITH ").append(sequence.getStartWith());
        ddl.append(" INCREMENT BY ").append(sequence.getIncrementBy());
        
        appendMinValue(ddl, sequence.getMinValue());
        appendMaxValue(ddl, sequence.getMaxValue());
        
        ddl.append(sequence.isCycle() ? " CYCLE" : " NOCYCLE");
        
        Integer cacheSize = sequence.getCacheSize();
        if (cacheSize == null) {
            ddl.append(" NOCACHE");
        } else {
            ddl.append(" CACHE ").append(cacheSize);
        }
        
        Boolean isOrder = sequence.getIsOrder();
        if (isOrder != null) {
            ddl.append(isOrder ? " ORDER" : " NOORDER");    
        }
        
        return ddl.toString();
    }

      private void appendMinValue(StringBuilder builder, BigDecimal minValue) {
        if (minValue != null) {
            builder.append(" MINVALUE ").append(minValue);
        }
    }

    private void appendMaxValue(StringBuilder builder, BigDecimal maxValue) {
        BigDecimal maxAllowedValue = new BigDecimal("9223372036854775807");
        if (maxValue != null && maxValue.compareTo(maxAllowedValue) <= 0) {
            builder.append(" MAXVALUE ").append(maxValue);
        } else {
            builder.append(" NOMAXVALUE");
        }
    }
}