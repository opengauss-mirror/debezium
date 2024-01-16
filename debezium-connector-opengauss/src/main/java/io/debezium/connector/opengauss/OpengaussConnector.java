/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.opengauss;

import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.connector.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.connector.common.RelationalBaseSourceConnector;
import io.debezium.connector.opengauss.connection.OpengaussConnection;
import io.debezium.relational.RelationalDatabaseConnectorConfig;

/**
 * A Kafka Connect source connector that creates tasks which use Postgresql streaming replication off a logical replication slot
 * to receive incoming changes for a database and publish them to Kafka.
 * <h2>Configuration</h2>
 * <p>
 * This connector is configured with the set of properties described in {@link OpengaussConnectorConfig}.
 *
 * @author Horia Chiorean
 */
public class OpengaussConnector extends RelationalBaseSourceConnector {

    private static final Logger LOGGER = LoggerFactory.getLogger(OpengaussConnector.class);
    private Map<String, String> props;

    public OpengaussConnector() {
    }

    @Override
    public String version() {
        return Module.version();
    }

    @Override
    public Class<? extends Task> taskClass() {
        return OpengaussConnectorTask.class;
    }

    @Override
    public void start(Map<String, String> props) {
        this.props = props;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        // this will always have just one task with the given list of properties
        return props == null ? Collections.emptyList() : Collections.singletonList(new HashMap<>(props));
    }

    @Override
    public void stop() {
        this.props = null;
    }

    @Override
    public ConfigDef config() {
        return OpengaussConnectorConfig.configDef();
    }

    @Override
    protected void validateConnection(Map<String, ConfigValue> configValues, Configuration config) {
        final ConfigValue databaseValue = configValues.get(RelationalDatabaseConnectorConfig.DATABASE_NAME.name());
        final ConfigValue slotNameValue = configValues.get(OpengaussConnectorConfig.SLOT_NAME.name());
        final ConfigValue pluginNameValue = configValues.get(OpengaussConnectorConfig.PLUGIN_NAME.name());
        if (!databaseValue.errorMessages().isEmpty() || !slotNameValue.errorMessages().isEmpty()
                || !pluginNameValue.errorMessages().isEmpty()) {
            return;
        }

        final OpengaussConnectorConfig postgresConfig = new OpengaussConnectorConfig(config);
        final ConfigValue hostnameValue = configValues.get(RelationalDatabaseConnectorConfig.HOSTNAME.name());
        // Try to connect to the database ...
        try (OpengaussConnection connection = new OpengaussConnection(postgresConfig.getJdbcConfig())) {
            try {
                // Prepare connection without initial statement execution
                connection.connection(false);
                // check connection
                connection.execute("SELECT version()");
                LOGGER.info("Successfully tested connection for {} with user '{}'", connection.connectionString(),
                        connection.username());
                // check server wal_level
                final String walLevel = connection.queryAndMap(
                        "SHOW wal_level",
                        connection.singleResultMapper(rs -> rs.getString("wal_level"), "Could not fetch wal_level"));
                if (!"logical".equals(walLevel)) {
                    final String errorMessage = "Postgres server wal_level property must be \"logical\" but is: " + walLevel;
                    LOGGER.error(errorMessage);
                    hostnameValue.addErrorMessage(errorMessage);
                }
                // check user for LOGIN and REPLICATION roles
                if (!connection.queryAndMap(
                        "SELECT r.rolsystemadmin AS rolsystemadmin, r.rolcanlogin AS rolcanlogin,"
                                + " r.rolreplication AS rolreplication FROM pg_roles r WHERE r.rolname = current_user",
                        connection.singleResultMapper(rs ->
                                        getSingleBooleanValue(rs.getString("rolsystemadmin"))
                                        || (getSingleBooleanValue(rs.getString("rolcanlogin"))
                                        && getSingleBooleanValue(rs.getString("rolreplication"))),
                                "could not fetch roles"))) {
                    final String errorMessage = "Postgres roles LOGIN and REPLICATION are not assigned to user: "
                            + connection.username();
                    LOGGER.error(errorMessage);
                } else {
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info("Privileges are met, postgres roles LOGIN and REPLICATION are assigned to user {}",
                                connection.username());
                    }
                }
            }
            catch (SQLException e) {
                LOGGER.error("Failed testing connection for {} with user '{}'", connection.connectionString(),
                        connection.username(), e);
                hostnameValue.addErrorMessage("Error while validating connector config: " + e.getMessage());
            }
        }
    }

    @Override
    protected Map<String, ConfigValue> validateAllFields(Configuration config) {
        return config.validate(OpengaussConnectorConfig.ALL_FIELDS);
    }

    /**
     * Get single boolean value
     *
     * @param value the value
     * @return boolean true or false
     */
    public static boolean getSingleBooleanValue(String value) {
        return value.equals("t") || value.equals("1");
    }
}
