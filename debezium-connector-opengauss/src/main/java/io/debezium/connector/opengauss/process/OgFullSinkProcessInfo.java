/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.opengauss.process;

import java.util.List;

/**
 * Description: OgFullSinkProcessInfo
 *
 * @author czy
 * @since 2023-06-07
 */
public class OgFullSinkProcessInfo {
    private TotalInfo total;
    private List<TableInfo> table;

    /**
     * Get
     *
     * @return total
     */
    public TotalInfo getTotal() {
        return total;
    }

    /**
     * Set
     *
     * @param total TotalInfo
     */
    public void setTotal(TotalInfo total) {
        this.total = total;
    }

    /**
     * Get
     *
     * @return table
     */
    public List<TableInfo> getTable() {
        return table;
    }

    /**
     * Set
     *
     * @param table List<TableInfo>
     */
    public void setTable(List<TableInfo> table) {
        this.table = table;
    }
}
