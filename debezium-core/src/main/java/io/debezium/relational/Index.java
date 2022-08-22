/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational;

import java.util.ArrayList;
import java.util.List;

/**
 * @author saxisuer
 * index struct
 */
public class Index {
    private String indexId;
    private String indexName;
    private String tableId;
    private String tableName;
    private String schemaName;

    private boolean unique;
    private List<IndexColumnExpr> indexColumnExpr = new ArrayList<>();

    public Index() {
    }

    public Index(Index indexChange) {
        if (null != indexChange) {
            this.indexColumnExpr = indexChange.getIndexColumnExpr();
            this.indexId = indexChange.indexId;
            this.indexName = indexChange.indexName;
            this.unique = indexChange.unique;
            this.schemaName = indexChange.schemaName;
            this.tableName = indexChange.tableName;
            this.tableId = indexChange.tableId;
        }
    }

    public String getIndexId() {
        return indexId;
    }

    public void setIndexId(String indexId) {
        this.indexId = indexId;
    }

    public String getIndexName() {
        return indexName;
    }

    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }

    public Boolean getUnique() {
        return unique;
    }

    public void setUnique(Boolean unique) {
        this.unique = unique;
    }

    public String getTableId() {
        return tableId;
    }

    public void setTableId(String tableId) {
        this.tableId = tableId;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public List<IndexColumnExpr> getIndexColumnExpr() {
        return indexColumnExpr;
    }

    public void setIndexColumnExpr(List<IndexColumnExpr> indexColumnExpr) {
        this.indexColumnExpr = indexColumnExpr;
    }

    public static class IndexColumnExpr {
        private boolean desc;
        private String columnExpr;
        private List<String> includeColumn = new ArrayList<>();

        public boolean isDesc() {
            return desc;
        }

        public void setDesc(boolean desc) {
            this.desc = desc;
        }

        public String getColumnExpr() {
            return columnExpr;
        }

        public void setColumnExpr(String columnExpr) {
            this.columnExpr = columnExpr;
        }

        public List<String> getIncludeColumn() {
            return includeColumn;
        }

        public void setIncludeColumn(List<String> includeColumn) {
            this.includeColumn = includeColumn;
        }
    }
}
