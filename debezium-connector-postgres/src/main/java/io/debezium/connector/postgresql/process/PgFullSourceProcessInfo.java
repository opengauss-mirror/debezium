/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package io.debezium.connector.postgresql.process;

import java.util.ArrayList;
import java.util.List;

/**
 * Description: PgFullSourceProcessInfo
 *
 * @author jianghongbo
 * @since 2025-02-05
 */
public class PgFullSourceProcessInfo {
    private int total;
    private List<ProgressInfo> progressList;

    /**
     * Constructor
     */
    public PgFullSourceProcessInfo() {
        this.progressList = new ArrayList<>();
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
    public List<ProgressInfo> getTableList() {
        return progressList;
    }

    /**
     * Set
     *
     * @param progressList List<TableInfo>
     */
    public void setTableList(List<ProgressInfo> progressList) {
        this.progressList = progressList;
    }
}
