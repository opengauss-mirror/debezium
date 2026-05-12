/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */

package org.full.migration.target.index;

import org.apache.commons.lang3.StringUtils;
import org.full.migration.model.table.TableIndex;
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
        if (tableIndex == null) {
            LOGGER.error("TableIndex object is null");
            return Optional.empty();
        }

        if (!isSupportedIndexType(tableIndex)) {
            return Optional.empty();
        }
        
        if (isFullTextIndexTable(tableIndex)) {
            return Optional.empty();
        }

        if (!StringUtils.isEmpty(tableIndex.getIndexDDL())) {
            return Optional.of(convertOracleDdlToOgrac(tableIndex.getIndexDDL()));
        }
        return Optional.empty();
    }


    private String convertOracleDdlToOgrac(String oracleDdl) {
        String ddl = oracleDdl.trim();
        if (ddl.endsWith(";")) {
            ddl = ddl.substring(0, ddl.length() - 1);
        }
        ddl = convertOracleDateFormat(ddl);
        ddl = convertOracleBitmapFormat(ddl);
        ddl = ddl.replaceAll("\\s+", " ").trim();
        return ddl;
    }

    /**
     * Convert Oracle BITMAP INDEX to oGRAC INDEX
     * @param ddl Oracle BITMAP INDEX statement
     * @return oGRAC INDEX statement
     */
    private String convertOracleBitmapFormat(String ddl) {
        ddl = ddl.replaceAll("CREATE BITMAP INDEX", "CREATE INDEX");
        return ddl;
    }

    private String convertOracleDateFormat(String ddl) {
        ddl = ddl.replaceAll("'DD-MON-RR'", "'YYYY-MM-DD'");
        ddl = ddl.replaceAll("'DD-MON-YY'", "'YYYY-MM-DD'");
        ddl = ddl.replaceAll("'DD-MON-YYYY'", "'YYYY-MM-DD'");
        ddl = ddl.replaceAll("'fmyyyy'", "'YYYY'");
        ddl = ddl.replaceAll("'yyyy'", "'YYYY'");
        ddl = ddl.replaceAll("'mm'", "'MM'");
        ddl = ddl.replaceAll("'dd'", "'DD'");
        return ddl;
    }

    @Override
    public Optional<String> getIndexSqlTemplate(TableIndex tableIndex) {
        return Optional.empty();
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
        
        return !indexType.equals("FUNCTION-BASED BITMAP") &&
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
}