/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */

package org.full.migration.test.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;

import java.io.FileInputStream;
import java.util.HashMap;
import java.util.Map;

/**
 * ConfigManager
 * Manages configuration loading and retrieval from YAML configuration files
 * This class provides methods to access various configuration parameters
 * needed for database connections and test environment management
 */
public class ConfigManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConfigManager.class);
    private final Map<String, Object> config;

    /**
     * Constructor
     * @param configFilePath Configuration file path
     */
    public ConfigManager(String configFilePath) {
        this.config = loadConfig(configFilePath);
    }

    /**
     * Load configuration from YAML file
     * @param configFilePath Configuration file path
     * @return Configuration map
     */
    private Map<String, Object> loadConfig(String configFilePath) {
        try (FileInputStream fis = new FileInputStream(configFilePath)) {
            // Use SafeConstructor to only allow standard YAML tags and block arbitrary
            // class instantiation via global tags (e.g. !!ClassName), mitigating the
            // unsafe deserialization risk (CVE-2022-1471). Plain maps/scalars are unaffected.
            Yaml yaml = new Yaml(new SafeConstructor(new LoaderOptions()));
            return yaml.load(fis);
        } catch (Exception e) {
            LOGGER.error("Error loading configuration: " + e.getMessage(), e);
            throw new RuntimeException("Failed to load configuration", e);
        }
    }

    /**
     * Get configuration value from a specific section
     * @param section Configuration section (e.g., "source", "target", "test")
     * @param key Configuration key
     * @param defaultValue Default value if not found
     * @return Configuration value
     */
    @SuppressWarnings("unchecked")
    public <T> T getConfigValue(String section, String key, T defaultValue) {
        Map<String, Object> sectionConfig = (Map<String, Object>) config.get(section);
        if (sectionConfig != null) {
            T value = (T) sectionConfig.get(key);
            if (value != null) {
                return value;
            }
        }
        return defaultValue;
    }

    /**
     * Get test scenario path from configuration
     * @return Test scenario path, defaults to "oracle_test_scenario"
     */
    public String getTestScenarioPath() {
        return getConfigValue("test", "scenarioPath", "oracle_test_scenario");
    }

    /**
     * Get test log configuration
     * @return Test log configuration, defaults to "disabled"
     */
    public String getTestLog() {
        return getConfigValue("test", "log", "disabled");
    }

    /**
     * Get database owner from configuration
     * Falls back to source username first, then target username
     * @return Database owner, defaults to "WANG"
     */
    public String getOwner() {
        String username = getConfigValue("source", "username", "");
        if (!username.isEmpty()) {
            return username;
        }
        username = getConfigValue("target", "username", "");
        if (!username.isEmpty()) {
            return username;
        }
        return "WANG";
    }

    /**
     * Get database type from connection URL
     * @param type Database type (source or target)
     * @return Database type (oracle or ograc)
     * @throws IllegalArgumentException if configuration for the specified type is not found
     */
    public String getDatabaseType(String type) {
        Object sectionConfigObj = config.get(type);
        if (sectionConfigObj == null) {
            throw new IllegalArgumentException("Configuration for database type " + type + " is not found");
        }
        if (!(sectionConfigObj instanceof Map)) {
            throw new IllegalArgumentException("Configuration for database type " + type + " is not a map");
        }
        Map<?, ?> sectionConfigRaw = (Map<?, ?>) sectionConfigObj;
        Map<String, Object> sectionConfig = new HashMap<>();
        for (Map.Entry<?, ?> entry : sectionConfigRaw.entrySet()) {
            if (entry.getKey() instanceof String) {
                sectionConfig.put((String) entry.getKey(), entry.getValue());
            }
        }
        String url = (String) sectionConfig.get("url");
        if (url.contains("oracle")) {
            return "oracle";
        } else if (url.contains("oGRAC")) {
            return "ograc";
        }
        return "unknown";
    }
}