/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package io.debezium.connector.postgresql.process;

import io.debezium.migration.ObjectEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * PgOfflineSinkProcessInfo
 *
 * @author jianghongbo
 * @since 2025-02-05
 */
public class PgOfflineSinkProcessInfo {
    private static final Logger LOGGER = LoggerFactory.getLogger(PgOfflineSinkProcessInfo.class);
    private TotalInfo total;
    private List<ProgressInfo> table = new ArrayList<>();

    private List<ProgressInfo> view = new ArrayList<>();

    private List<ProgressInfo> function = new ArrayList<>();

    private List<ProgressInfo> trigger = new ArrayList<>();

    private List<ProgressInfo> procedure = new ArrayList<>();

    /**
     * Get
     *
     * @return TotalInfo
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
     * @return List<ProgressInfo> of table
     */
    public List<ProgressInfo> getTable() {
        return table;
    }

    /**
     * Get
     *
     * @return List<ProgressInfo> of view
     */
    public List<ProgressInfo> getView() {
        return view;
    }

    /**
     * Get
     *
     * @return List<ProgressInfo> of function
     */
    public List<ProgressInfo> getFunction() {
        return function;
    }

    /**
     * Get
     *
     * @return List<ProgressInfo> of trigger
     */
    public List<ProgressInfo> getTrigger() {
        return trigger;
    }

    /**
     * Get
     *
     * @return List<ProgressInfo> of procedure
     */
    public List<ProgressInfo> getProcedure() {
        return procedure;
    }

    /**
     * Set
     *
     * @param table List<TableInfo>
     */
    public void setTable(List<ProgressInfo> table) {
        this.table = table;
    }

    /**
     * add Table progress
     *
     * @param progressInfo ProgressInfo
     */
    public void addTable(ProgressInfo progressInfo) {
        if (table == null) {
            table = new ArrayList<>();
        }
        table.add(progressInfo);
    }

    /**
     * add object progress by objType
     *
     * @param progressInfo ProgressInfo
     * @param objType ObjectEnum
     */
    public void addObject(ProgressInfo progressInfo, ObjectEnum objType) {
        switch (objType) {
            case FUNCTION:
                function.add(progressInfo);
                break;
            case VIEW:
                view.add(progressInfo);
                break;
            case TRIGGER:
                trigger.add(progressInfo);
                break;
            case PROCEDURE:
                procedure.add(progressInfo);
                break;
            default:
                LOGGER.error("unknown object Type");
        }
    }
}
