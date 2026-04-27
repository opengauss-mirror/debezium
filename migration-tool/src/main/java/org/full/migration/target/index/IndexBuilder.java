/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */

package org.full.migration.target.index;

import org.full.migration.model.table.TableIndex;
import java.util.Optional;

/**
 * IndexBuilder interface for building index SQL statements
 */
public interface IndexBuilder {
    /**
     * Builds create index SQL statement for the given table index
     * 
     * @param tableIndex the table index object
     * @return Optional of create index SQL statement
     */
    Optional<String> buildCreateIndexSql(TableIndex tableIndex);
    
    /**
     * Gets index SQL template based on index type
     * 
     * @param tableIndex the table index object
     * @return Optional of index SQL template
     */
    Optional<String> getIndexSqlTemplate(TableIndex tableIndex);
}