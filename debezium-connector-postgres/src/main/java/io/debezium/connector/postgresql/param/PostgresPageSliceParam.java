/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package io.debezium.connector.postgresql.param;

/**
 * page and slice parameter info
 *
 * @author jianghongbo
 * @since 2025/2/9
 */
public class PostgresPageSliceParam {
    private int pageRows;
    private long realTotalRows = 0L;
    private int subscript = 1;
    private int totalSlice = -1;
    private double spacePerSlice = -1;

    /**
     * constructor
     */
    public PostgresPageSliceParam() {}

    /**
     * Gets
     *
     * @return int pageRows
     */
    public int getPageRows() {
        return pageRows;
    }

    /**
     * Gets
     *
     * @return long realTotalRows
     */
    public long getRealTotalRows() {
        return realTotalRows;
    }

    /**
     * Gets
     *
     * @return int subscript
     */
    public int getSubscript() {
        return subscript;
    }

    /**
     * Gets
     *
     * @return int totalSlice
     */
    public int getTotalSlice() {
        return totalSlice;
    }

    /**
     * Gets
     *
     * @return double spacePerSlice
     */
    public double getSpacePerSlice() {
        return spacePerSlice;
    }

    /**
     * Sets
     *
     * @param pageRows int
     */
    public void setPageRows(int pageRows) {
        this.pageRows = pageRows;
    }

    /**
     * add real total rows
     *
     * @param rows long
     */
    public void addRealTotalRows(long rows) {
        this.realTotalRows += rows;
    }

    /**
     * add subscript
     *
     * @param slices long
     */
    public void addSubscript(int slices) {
        this.subscript += slices;
    }

    /**
     * Sets
     *
     * @param totalSlice int
     */
    public void setTotalSlice(int totalSlice) {
        this.totalSlice = totalSlice;
    }

    /**
     * Sets
     *
     * @param spacePerSlice double
     */
    public void setSpacePerSlice(double spacePerSlice) {
        this.spacePerSlice = spacePerSlice;
    }
}
