/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.sink.object;

/**
 * Description: DataOperation class
 * @author douxin
 * @date 2022/10/31
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
     * @param boolean true if is dml
     */
    public void setIsDml(boolean isDml) {
        this.isDml = isDml;
    }
}
