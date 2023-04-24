/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.process;

import io.debezium.connector.process.BaseSourceProcessInfo;

/**
 * Description: MysqlSourceProcessInfo
 *
 * @author wangzhengyuan
 * @since 2023-03-30
 */
public class MysqlSourceProcessInfo extends BaseSourceProcessInfo {
    /**
     * SourceProcessInfo the sourceProcessInfo
     */
    public static final MysqlSourceProcessInfo SOURCE_PROCESS_INFO = new MysqlSourceProcessInfo();

    private MysqlSourceProcessInfo() {
    }
}
