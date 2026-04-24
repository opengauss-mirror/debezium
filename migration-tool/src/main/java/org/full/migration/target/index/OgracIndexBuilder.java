/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */

package org.full.migration.target.index;

import org.apache.commons.lang3.StringUtils;
import org.full.migration.model.table.TableIndex;
import org.full.migration.utils.DatabaseUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Locale;
import java.util.Optional;

/**
 * OgracIndexBuilder implementation for building index SQL statements for oGRAC
 */
public class OgracIndexBuilder implements IndexBuilder {
    private static final Logger LOGGER = LoggerFactory.getLogger(OgracIndexBuilder.class);
    
    @Override
    public Optional<String> buildCreateIndexSql(TableIndex tableIndex) {
        // Validate input
        if (tableIndex == null) {
            LOGGER.error("TableIndex object is null");
            return Optional.empty();
        }

        // Check column count limit (maximum 16 columns)
        String indexColumns = StringUtils.isEmpty(tableIndex.getIndexprs())
                ? tableIndex.getColumnName()
                : tableIndex.getIndexprs();

        // Count columns in composite index
        if (!tableIndex.isConstraint()) {
            String[] columns = indexColumns.split(",");
            if (columns.length > 16) {
                LOGGER.error("Index exceeds maximum column count (16) limit, schema:{}, table:{}, name:{}",
                        tableIndex.getSchemaName(), tableIndex.getTableName(), tableIndex.getIndexName());
                return Optional.empty();
            }

            // Check for LOB/ARRAY/IMAGE types in non-function indexes
            if (StringUtils.isEmpty(tableIndex.getIndexprs())) {
                // This is a regular index, not a function index
                // We should check if any column is of LOB/ARRAY/IMAGE type
                // For simplicity, we'll assume these types are not allowed
                // In a real implementation, we would need to check column types from metadata
            }

            // Check function index constraints
            if (!StringUtils.isEmpty(tableIndex.getIndexprs())) {
                if (!isValidFunctionIndex(tableIndex.getIndexprs())) {
                    return Optional.empty();
                }
            }
        }

        Optional<String> indexSqlTempOptional = getIndexSqlTemplate(tableIndex);
        if (indexSqlTempOptional.isPresent()) {
            StringBuilder builder;

            if (!tableIndex.isConstraint()) {
                builder = new StringBuilder(
                        String.format(indexSqlTempOptional.get(),
                                DatabaseUtils.formatObjName(tableIndex.getIndexName()),
                                DatabaseUtils.formatObjName(tableIndex.getSchemaName()),
                                DatabaseUtils.formatObjName(tableIndex.getTableName()),
                                indexColumns));
            } else {
                builder = new StringBuilder(
                        String.format(indexSqlTempOptional.get(),
                                DatabaseUtils.formatObjName(tableIndex.getSchemaName()),
                                DatabaseUtils.formatObjName(tableIndex.getTableName()),
                                DatabaseUtils.formatObjName(tableIndex.getIndexName()),
                                indexColumns));
            }

            return Optional.of(builder.toString());
        }
        return Optional.empty();
    }

    @Override
    public Optional<String> getIndexSqlTemplate(TableIndex tableIndex) {
        if (tableIndex == null) {
            return Optional.empty();
        }
        
        if (!isSupportedIndexType(tableIndex)) {
            return Optional.empty();
        }
        
        if (isFullTextIndexTable(tableIndex)) {
            return Optional.empty();
        }
        
        StringBuilder templateBuilder = buildBaseIndexTemplate(tableIndex);
        appendIndexSpecificOptions(templateBuilder, tableIndex);
        
        String createIndexTemp = templateBuilder.toString().trim();
        return Optional.of(createIndexTemp);
    }
    
    /**
     * Check if the index type is supported by oGRAC
     * @param tableIndex TableIndex object
     * @return true if the index type is supported
     */
    private boolean isSupportedIndexType(TableIndex tableIndex) {
        String indexType = tableIndex.getIndexType() == null ? "" : tableIndex.getIndexType().toUpperCase(Locale.ROOT);
        if (StringUtils.isEmpty(indexType)) {
            return false;
        }
        
        return !indexType.equals("BITMAP") && 
               !indexType.equals("FUNCTION-BASED BITMAP") && 
               !indexType.equals("DOMAIN");
    }
    
    /**
     * Check if the table is related to full-text index (starts with DR$)
     * @param tableIndex TableIndex object
     * @return true if the table is related to full-text index
     */
    private boolean isFullTextIndexTable(TableIndex tableIndex) {
        return tableIndex.getTableName().startsWith("DR$");
    }
    
    /**
     * Build the base index SQL template
     * @param tableIndex TableIndex object
     * @return StringBuilder with base template
     */
    private StringBuilder buildBaseIndexTemplate(TableIndex tableIndex) {
        StringBuilder templateBuilder = new StringBuilder("CREATE ");
        
        if (tableIndex.isUnique()) {
            templateBuilder.append("UNIQUE ");
        }
        
        templateBuilder.append("INDEX ")
                .append("IF NOT EXISTS ")
                .append("%s ")
                .append("ON ")
                .append("%s.%s ")
                .append("(%s) ");
        
        return templateBuilder;
    }
    
    /**
     * Append index-specific options to the template
     * @param templateBuilder StringBuilder to append to
     * @param tableIndex TableIndex object
     */
    private void appendIndexSpecificOptions(StringBuilder templateBuilder, TableIndex tableIndex) {
        String indexType = tableIndex.getIndexType() == null ? "" : tableIndex.getIndexType().toUpperCase(Locale.ROOT);
        if (indexType.contains("NORMAL/REV")) {
            templateBuilder.append("REVERSE ");
        }
        
        if (!tableIndex.isConstraint()) {
            String indexRange = tableIndex.getIndexRange() != null
                    ? tableIndex.getIndexRange().toUpperCase(Locale.ROOT)
                    : null;
            
            if ("LOCAL".equals(indexRange)) {
                templateBuilder.append("LOCAL ");
            } else if ("GLOBAL".equals(indexRange)) {
                templateBuilder.append("GLOBAL ");
            }
        }
    }
    
    /**
     * Check if the function index is valid
     * @param indexExpr the index expression
     * @return true if the function index is valid
     */
    private boolean isValidFunctionIndex(String indexExpr) {
        // Check if functions used are supported
        String[] supportedFunctions = {
                "abs", "decode", "jsonb_value", "json_value", "lower",
                "nvl", "nvl2", "radians", "regexp_instr", "regexp_substr",
                "reverse", "substr", "substrb", "to_char", "to_date",
                "to_number", "trim", "trunc", "upper"
        };

        // Check for unsupported functions
        boolean hasUnsupportedFunction = false;
        String unsupportedFunction = null;

        // Convert to lowercase for case-insensitive matching
        String lowerExpr = indexExpr.toLowerCase(Locale.ROOT);

        // Extract all function calls from the expression
        // This is a simplified approach - in a real implementation, we would need
        // to properly parse the expression to identify functions
        java.util.regex.Pattern pattern = java.util.regex.Pattern.compile("\\b([a-zA-Z_][a-zA-Z0-9_]*)\\s*\\(");
        java.util.regex.Matcher matcher = pattern.matcher(lowerExpr);

        while (matcher.find()) {
            String functionName = matcher.group(1);
            boolean isSupported = false;
            for (String supportedFunc : supportedFunctions) {
                if (supportedFunc.equals(functionName)) {
                    isSupported = true;
                    break;
                }
            }
            if (!isSupported) {
                hasUnsupportedFunction = true;
                unsupportedFunction = functionName;
                break;
            }
        }

        if (hasUnsupportedFunction) {
            LOGGER.error("Index uses unsupported function '{}'", unsupportedFunction);
            return false;
        }
        
        return true;
    }
}