/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */

package org.full.migration.utils;

import org.full.migration.datax.config.DataXCommonConfig;
import org.full.migration.exception.ConfigurationException;
import org.full.migration.exception.ErrorCode;
import org.full.migration.model.config.DataXParamConfig;
import org.full.migration.model.config.DatabaseConfig;
import org.full.migration.model.config.GlobalConfig;

public class DataXConfigUtils {
    /**
     * loadCommonConfig
     * Loads the common DataX configuration from global config
     *
     * @param globalConfig Global configuration object
     * @return Common DataX configuration object
     * @throws ConfigurationException If an error occurs during configuration
     *                                loading or conversion
     */
    public static DataXCommonConfig loadCommonConfig(GlobalConfig globalConfig) throws ConfigurationException {
        DataXCommonConfig config = new DataXCommonConfig();
        if (globalConfig != null && globalConfig.getDatax() != null) {
            DataXParamConfig dataxConfig = globalConfig.getDatax();
            try {
                copyNonNullProperties(dataxConfig, config);
            } catch (Exception e) {
                throw new ConfigurationException(ErrorCode.DATAX_CONFIG_ERROR.getCode(),
                        "DataX配置错误,Failed to load DataX config properties: " + e.getMessage(), e);
            }
        }
        return config;
    }

    /**
     * Prepare DataX common configuration : config reader and writer properties :
     * jdbcUrl, username, password
     *
     * @param sourceConfig Source database configuration
     * @param targetConfig Target database configuration
     */
    public static void prepareCommonConfig(DataXCommonConfig config, DatabaseConfig sourceConfig,
                                           DatabaseConfig targetConfig) {
        if (config.getReaderJdbcUrl() == null) {
            String readerJdbcUrl = generateReaderJdbcUrl(sourceConfig);
            config.setReaderJdbcUrl(readerJdbcUrl);
        }
        if (config.getReaderUsername() == null) {
            config.setReaderUsername(sourceConfig.getUser());
        }
        if (config.getReaderPassword() == null) {
            config.setReaderPassword(sourceConfig.getPassword());
        }
        if (config.getWriterJdbcUrl() == null) {
            String writerJdbcUrl = generateWriterJdbcUrl(targetConfig);
            config.setWriterJdbcUrl(writerJdbcUrl);
        }
        if (config.getWriterUsername() == null) {
            config.setWriterUsername(targetConfig.getUser());
        }
        if (config.getWriterPassword() == null) {
            config.setWriterPassword(targetConfig.getPassword());
        }
    }

    /**
     * generateReaderJdbcUrl Default to using oracle
     * Generate corresponding jdbcUrl based on readerName
     * Adds common default parameters if not specified
     *
     * @param sourceConfig Source database configuration
     * @return jdbcUrl
     */
    private static String generateReaderJdbcUrl(DatabaseConfig sourceConfig) {
        return JdbcUtils.generateOracleJdbcUrl(sourceConfig);
    }

    /**
     * generateWriterJdbcUrl  Default to using oGRAC
     * Generate corresponding jdbcUrl based on writerName
     *
     * @param targetConfig Target database configuration
     * @return jdbcUrl
     */
    private static String generateWriterJdbcUrl(DatabaseConfig targetConfig) {
        return JdbcUtils.generateOgracJdbcUrl(targetConfig);
    }

    /**
     * Copies only non-null properties from source to target
     *
     */
    private static void copyNonNullProperties(Object source, Object target) throws Exception {
        if (source == null || target == null) {
            return;
        }
        java.lang.reflect.Field[] sourceFields = source.getClass().getDeclaredFields();
        for (java.lang.reflect.Field sourceField : sourceFields) {
            sourceField.setAccessible(true);
            Object value = sourceField.get(source);
            if (value != null) {
                try {
                    java.lang.reflect.Field targetField = target.getClass().getDeclaredField(sourceField.getName());
                    targetField.setAccessible(true);
                    targetField.set(target, value);
                } catch (NoSuchFieldException e) {
                    // Field doesn't exist in target, skip
                }
            }
        }
    }
}
