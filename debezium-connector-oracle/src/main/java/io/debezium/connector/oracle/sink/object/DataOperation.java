/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.sink.object;

/**
 * Description: DataOperation class
 * @author gbase
 * @date 2023/07/28
 **/
public abstract class DataOperation {
    /**
     * Is dml flag
     */
    protected boolean isDml;

    /**
     * Gets is dml
     *
     * @return boolean true if is dml
     */
    public boolean getIsDml() {
        return isDml;
    }

    /**
     * Sets is dml
     *
     * @param isDml true if is dml
     */
    public void setIsDml(boolean isDml) {
        this.isDml = isDml;
    }
}
