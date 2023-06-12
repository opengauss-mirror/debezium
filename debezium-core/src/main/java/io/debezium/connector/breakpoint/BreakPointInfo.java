/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.breakpoint;

/**
 * Description: BreakPointInfo
 *
 * @author Lvlintao
 * @since 2023/7/24
 **/

public class BreakPointInfo {
    private String key;
    private String value;

    /**
     * Gets key.
     *
     * @return the value of key
     */
    public String getKey() {
        return key;
    }

    /**
     * Sets the key.
     *
     * @param key key
     * @return breakpointInfo the breakpointInfo
     */
    public BreakPointInfo setKey(String key) {
        this.key = key;
        return this;
    }

    /**
     * Gets value.
     *
     * @return the value of value
     */
    public String getValue() {
        return value;
    }

    /**
     * Sets the value.
     *
     * @param value value
     * @return breakpointInfo the breakpointInfo
     */
    public BreakPointInfo setValue(String value) {
        this.value = value;
        return this;
    }
}
