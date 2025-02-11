/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package io.debezium.connector.postgresql.sink;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.postgresql.sink.object.ConnectionInfo;
import io.debezium.connector.postgresql.sink.object.ForeignKeyMetaData;
import io.debezium.connector.postgresql.sink.task.PostgresSinkConnectorConfig;
import io.debezium.connector.postgresql.sink.task.PostgresSinkConnectorTask;
import io.debezium.util.Strings;

/**
 * PostgresSinkDetachConnector
 *
 * @author tianbin
 * @since 2025-01-14
 */
public class PostgresSinkDetachConnector extends SinkConnector {
    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresSinkDetachConnector.class);
    private static final ConfigDef CONFIG_DEF = PostgresSinkConnectorConfig.CONFIG_DEF;

    private volatile AtomicBoolean isConnectionAlive = new AtomicBoolean(true);

    @Override
    public void start(Map<String, String> props) {
        PostgresSinkConnectorConfig config = new PostgresSinkConnectorConfig(props);
        ConnectionInfo connectionInfo = new ConnectionInfo(config, isConnectionAlive);
        Connection connection = connectionInfo.createOpenGaussConnection();
        List<ForeignKeyMetaData> fkList = new ArrayList<>();
        addForeignKeyConstraint(connection, fkList);
        validateForeignKeyConstraint(connection, fkList);
        try {
            connection.close();
        } catch (SQLException e) {
            LOGGER.error("An error occurred while closing the connection: ", e);
        }
    }

    private void addForeignKeyConstraint(Connection connection, List<ForeignKeyMetaData> fkList) {
        try (PreparedStatement ps = connection.prepareStatement("select table_name, table_schema, constraint_name,"
                + " referenced_table_name, referenced_table_schema, fk_cols, ref_columns, update_rule, delete_rule"
                + " from sch_debezium.pg_fkeys");
             ResultSet rs = ps.executeQuery();) {
            while (rs.next()) {
                String tableName = rs.getString("table_name");
                String schema = rs.getString("table_schema");
                String fkName = rs.getString("constraint_name");
                String referencedTableName = rs.getString("referenced_table_name");
                String referencedSchema = rs.getString("referenced_table_schema");
                String fkCols = rs.getString("fk_cols");
                String updateRule = rs.getString("update_rule");
                String deleteRule = rs.getString("delete_rule");
                String refColumns = rs.getString("ref_columns");
                ForeignKeyMetaData fkMetaData = new ForeignKeyMetaData(schema, tableName, fkName);
                fkList.add(fkMetaData);
                String foreignKeySql = String.format("ALTER TABLE %s.%s ADD CONSTRAINT %s FOREIGN KEY (%s) "
                                + "REFERENCES %s.%s (%s) ON UPDATE %s ON DELETE %s NOT VALID;",
                        Strings.toDoubleQuotedString(schema),
                        Strings.toDoubleQuotedString(tableName),
                        Strings.toDoubleQuotedString(fkName),
                        fkCols,
                        Strings.toDoubleQuotedString(referencedSchema),
                        Strings.toDoubleQuotedString(referencedTableName),
                        refColumns, updateRule, deleteRule);
                LOGGER.info("creating invalid foreign key {} on table {}.{}", fkName, schema, tableName);
                connection.createStatement().execute(foreignKeySql);
            }
        } catch (SQLException exp) {
            LOGGER.error("An error occurred while adding foreign key constraint: ", exp);
        }
    }

    private void validateForeignKeyConstraint(Connection connection, List<ForeignKeyMetaData> fkList) {
        try {
            for (ForeignKeyMetaData foreignKeyMetaData : fkList) {
                Statement statement = connection.createStatement();
                String fkName = foreignKeyMetaData.getFkName();
                String schema = foreignKeyMetaData.getSchema();
                String tableName = foreignKeyMetaData.getTableName();
                String sql = String.format("ALTER TABLE %s.%s VALIDATE CONSTRAINT %s;",
                        Strings.toDoubleQuotedString(schema),
                        Strings.toDoubleQuotedString(tableName),
                        Strings.toDoubleQuotedString(fkName));
                LOGGER.info("validating {} on table {}.{}", fkName, schema, tableName);
                statement.execute(sql);
            }
            LOGGER.info("all table constraints have been successfully validated");
        } catch (SQLException exp) {
            LOGGER.error("An error occurred while validating foreign key constraint: ", exp);
        }
    }

    @Override
    public Class<? extends Task> taskClass() {
        return PostgresSinkConnectorTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        // No Task needs to be started
        return Collections.emptyList();
    }

    @Override
    public void stop() {
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public String version() {
        return "1.0.0";
    }
}
