/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.sink.object;

import org.apache.kafka.connect.data.Struct;

/**
 * Description: DdlField
 * @author douxin
 * @date 2022/10/28
 **/
public class DdlOperation extends DataOperation {
    /**
     * Ddl
     */
    public static final String DDL = "ddl";

    private String ddl;

    /**
     * Constructor
     *
     * @param Struct the value
     */
    public DdlOperation(Struct value) {
        if (value == null) {
            throw new IllegalArgumentException("value can't be null!");
        }
        this.ddl = value.getString(DdlOperation.DDL);
        setIsDml(false);
    }

    /**
     * Constructor
     *
     * @param String the ddl
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
     * @param String the ddl
     */
    public void setDdl(String ddl) {
        this.ddl = ddl;
    }

    @Override
    public String toString() {
        return "DdlOperation{" +
                "isDml=" + isDml +
                ", ddl='" + ddl + '\'' +
                '}';
    }
}
