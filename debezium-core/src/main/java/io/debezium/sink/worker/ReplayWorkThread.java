/*
 * Copyright 2026 Huawei Technologies Co., Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.debezium.sink.worker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * base replay workthread
 *
 * @author jianghongbo
 * @since 2024/11/26
 */
public class ReplayWorkThread extends Thread {
    private static final Logger LOGGER = LoggerFactory.getLogger(ReplayWorkThread.class);
    private static final int MAX_CREATESTMT_COUNT = 5000;

    private boolean isClearFile;
    private boolean isStop = false;
    private Connection connection;

    public ReplayWorkThread(int index) {
        super("work-thread-" + index);
    }

    /**
     * set isDelCsv
     *
     * @param isDelCsv boolean
     */
    public void setClearFile(boolean isDelCsv) {
        this.isClearFile = isDelCsv;
    }

    /**
     * set isStop
     *
     * @param isStop boolean
     */
    public void setIsStop(boolean isStop) {
        this.isStop = isStop;
    }

    /**
     * get jdbc connection
     *
     * @return Connection
     */
    public Connection getConnection() {
        return connection;
    }

    /**
     * set jdbc connection
     *
     * @param connection Connection
     */
    public void setConnection(Connection connection) {
        this.connection = connection;
    }

    /**
     * create statement recursivly
     *
     * @param connection Connection
     * @param tryCount int
     * @return Statement
     */
    protected Statement createStatement(Connection connection, int tryCount) {
        Statement statement = null;
        if (tryCount > MAX_CREATESTMT_COUNT) {
            return statement;
        }
        try {
            statement = connection.createStatement();
            LOGGER.info("create statement info success");
        } catch (SQLException e) {
            LOGGER.warn("create statement failed, try {} times", tryCount);
            statement = createStatement(connection, tryCount + 1);
        }
        return statement;
    }
}
