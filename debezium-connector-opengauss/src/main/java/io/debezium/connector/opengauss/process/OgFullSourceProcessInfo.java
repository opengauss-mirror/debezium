/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.opengauss.process;

import java.util.ArrayList;
import java.util.List;

/**
 * Description: OgFullSourceProcessInfo
 *
 * @author czy
 * @since 2023-06-07
 */
public class OgFullSourceProcessInfo {
    private int total;
    private List<TableInfo> tableList;

    /**
     * Constructor
     */
    public OgFullSourceProcessInfo() {
        this.tableList = new ArrayList<>();
    }

    /**
     * Get
     *
     * @return int
     */
    public int getTotal() {
        return total;
    }

    /**
     * Set
     *
     * @param total int
     */
    public void setTotal(int total) {
        this.total = total;
    }

    /**
     * Get
     *
     * @return List<TableInfo>
     */
    public List<TableInfo> getTableList() {
        return tableList;
    }

    /**
     * Set
     *
     * @param tableList List<TableInfo>
     */
    public void setTableList(List<TableInfo> tableList) {
        this.tableList = tableList;
    }
}
