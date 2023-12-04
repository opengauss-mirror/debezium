/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.sink.object;

import org.apache.kafka.connect.data.Struct;

/**
 * Description: DdlOperation
 * @author gbase
 * @date 2023/07/28
 **/
public class DdlOperation extends DataOperation {
    /**
     * Ddl
     */
    public static final String DDL = "ddl";

    private String ddl;

    /**
     * tableChanges
     */
    private TableChangesField tableChangesField;

    /**
     * Constructor
     *
     * @param value the value
     */
    public DdlOperation(Struct value) {
        if (value == null) {
            throw new IllegalArgumentException("value can't be null!");
        }
        this.ddl = value.getString(DdlOperation.DDL);
        this.tableChangesField = new TableChangesField(value);
        setIsDml(false);
    }

    /**
     * Constructor
     *
     * @param ddl the ddl
     */
    public DdlOperation(String ddl) {
        this.ddl = ddl;
        setIsDml(false);
    }

    /**
     * Gets ddl
     *
     * @return String the ddl
     */
    public String getDdl() {
        return this.ddl + ";";
    }

    /**
     * Sets ddl
     *
     * @param ddl the ddl
     */
    public void setDdl(String ddl) {
        this.ddl = ddl;
    }

    /**
     * Gets tableChanges field
     *
     * @return TableChangesField
     */
    public TableChangesField getTableChangesField() {
        return tableChangesField;
    }

    /**
     * Sets tableChanges field
     *
     * @param tableChangesField tableChanges
     */
    public void setTableChangesField(TableChangesField tableChangesField) {
        this.tableChangesField = tableChangesField;
    }

    @Override
    public String toString() {
        return "DdlOperation{" +
                "ddl='" + ddl + '\'' +
                ", tableChangesField=" + tableChangesField +
                ", isDml=" + isDml +
                '}';
    }
}
