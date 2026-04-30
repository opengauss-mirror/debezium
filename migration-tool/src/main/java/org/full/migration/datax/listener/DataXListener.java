/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */

package org.full.migration.datax.listener;

import org.full.migration.coordinator.ProgressTracker;
import org.full.migration.model.progress.ProgressInfo;
import org.full.migration.model.progress.ProgressStatus;
import org.full.migration.utils.MigrationErrorLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * DataXListener
 * DataX listener, used to handle DataX execution events
 *
 * @since 2025-04-18
 */
public class DataXListener {
    private static final Logger LOGGER = LoggerFactory.getLogger(DataXListener.class);
    private static final DataXListener INSTANCE = new DataXListener();
    private static final Pattern LOG_LINE_PATTERN = Pattern
            .compile("^\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d{3} \\[.*\\] (INFO|ERROR|WARN|DEBUG|TRACE)");
    private static final Pattern PROGRESS_PATTERN = Pattern.compile("(\\d+)%\\s+\\(.*\\)");
    private static final Pattern SPEED_PATTERN = Pattern.compile("速度.*(\\d+\\.\\d+)\\s+records/s");
    private static final Pattern RECORDS_PATTERN = Pattern.compile("记录数.*(\\d+)\\s+条");
    private static final Pattern ERROR_PATTERN = Pattern.compile("(ERROR|Exception).*:(.*)");
    private static final Pattern SUCCESS_PATTERN = Pattern.compile("任务执行成功");
    private static final Pattern FAILURE_PATTERN = Pattern.compile("任务执行失败");

    private boolean isEnableOutputDataxLogs = false;
    private boolean isDumpJson = false;
    private final List<LogLineProcessor> processors = new ArrayList<>();
    // 错误信息缓存，key为表名，value为错误信息列表
    private final Map<String, List<String>> errorCache = new HashMap<>();

    /**
     * LogLineProcessor functional interface for processing log lines
     */
    @FunctionalInterface
    private interface LogLineProcessor {
        /**
         * Process a log line
         * 
         * @param line          the log line
         * @param fullTableName the full table name
         * @return true if the line was processed
         */
        boolean process(String line, String fullTableName);
    }

    private DataXListener() {
        LOGGER.info("DataXListener initialized");
        initializeProcessors();
    }

    /**
     * Initialize log line processors
     */
    private void initializeProcessors() {
        processors.add(this::processProgressLine);
        processors.add(this::processSpeedLine);
        processors.add(this::processRecordsLine);
        processors.add(this::processErrorLine);
        processors.add(this::processSuccessLine);
        processors.add(this::processFailureLine);
    }

    public static DataXListener getInstance() {
        return INSTANCE;
    }

    /**
     * isEnable output datax logs
     *
     * @param isEnableOutputDataxLogs isEnableOutputDataxLogs
     */
    public void setEnableOutputDataxLogs(boolean isEnableOutputDataxLogs) {
        this.isEnableOutputDataxLogs = isEnableOutputDataxLogs;
    }

    /**
     * setIsDumpJson
     *
     * @param isDumpJson isDumpJson
     */
    public void setIsDumpJson(boolean isDumpJson) {
        this.isDumpJson = isDumpJson;
    }

    /**
     * Process DataX output line
     *
     * @param line       Output line
     * @param schemaName Schema name
     * @param tableName  Table name
     */
    public void processOutputLine(String line, String schemaName, String tableName) {
        if (line == null || line.isEmpty()) {
            return;
        }
        String fullTableName = schemaName + "." + tableName;
        boolean isProcessed = processors.stream()
                .anyMatch(processor -> processor.process(line, fullTableName));
        if (!isProcessed && isEnableOutputDataxLogs) {
            this.outputDataxLogs(line, fullTableName);
        }
    }

    /**
     * Process progress line
     *
     * @param line          Output line
     * @param fullTableName Full table name
     * @return true if the line was processed
     */
    private boolean processProgressLine(String line, String fullTableName) {
        Matcher progressMatcher = PROGRESS_PATTERN.matcher(line);
        if (progressMatcher.find()) {
            try {
                int progress = Integer.parseInt(progressMatcher.group(1));
                updateProgress(fullTableName, progress, ProgressStatus.IN_MIGRATED.getCode());
                LOGGER.debug("Progress for {}: {}%", fullTableName, progress);
            } catch (NumberFormatException e) {
                LOGGER.warn("Failed to parse progress: {}", line);
            }
            return true;
        }
        return false;
    }

    /**
     * Process speed line
     *
     * @param line          Output line
     * @param fullTableName Full table name
     * @return true if the line was processed
     */
    private boolean processSpeedLine(String line, String fullTableName) {
        Matcher speedMatcher = SPEED_PATTERN.matcher(line);
        if (speedMatcher.find()) {
            String speed = speedMatcher.group(1);
            LOGGER.debug("Speed for {}: {} records/s", fullTableName, speed);
            return true;
        }
        return false;
    }

    /**
     * Process records line
     *
     * @param line          Output line
     * @param fullTableName Full table name
     * @return true if the line was processed
     */
    private boolean processRecordsLine(String line, String fullTableName) {
        Matcher recordsMatcher = RECORDS_PATTERN.matcher(line);
        if (recordsMatcher.find()) {
            String records = recordsMatcher.group(1);
            LOGGER.debug("Records processed for {}: {}", fullTableName, records);
            return true;
        }
        return false;
    }

    /**
     * Process error line
     *
     * @param line          Output line
     * @param fullTableName Full table name
     * @return true if the line was processed
     */
    private boolean processErrorLine(String line, String fullTableName) {
        Matcher errorMatcher = ERROR_PATTERN.matcher(line);
        if (errorMatcher.find()) {
            LOGGER.error("Error for {}: {}", fullTableName, line);
            // 将错误信息添加到缓存中
            errorCache.computeIfAbsent(fullTableName, k -> new ArrayList<>()).add(line);
            return true;
        }
        return false;
    }

    /**
     * Process success line
     *
     * @param line          Output line
     * @param fullTableName Full table name
     * @return true if the line was processed
     */
    private boolean processSuccessLine(String line, String fullTableName) {
        if (SUCCESS_PATTERN.matcher(line).find()) {
            LOGGER.info("Migration successful for {}", fullTableName);
            updateProgress(fullTableName, 100, ProgressStatus.MIGRATED_COMPLETE.getCode());
            return true;
        }
        return false;
    }

    /**
     * Process failure line
     *
     * @param line          Output line
     * @param fullTableName Full table name
     * @return true if the line was processed
     */
    private boolean processFailureLine(String line, String fullTableName) {
        if (FAILURE_PATTERN.matcher(line).find()) {
            LOGGER.error("Migration failed for {}", fullTableName);
            updateProgress(fullTableName, 0, ProgressStatus.MIGRATED_FAILURE.getCode());
            return true;
        }
        return false;
    }

    private void outputDataxLogs(String line, String fullTableName) {
        boolean isStandardLogLine = LOG_LINE_PATTERN.matcher(line).find();
        if (isStandardLogLine) {
            if (line.contains(" ERROR ")) {
                LOGGER.error(" {}: {}", fullTableName, line);
                // 将错误信息添加到缓存中
                errorCache.computeIfAbsent(fullTableName, k -> new ArrayList<>()).add(line);
            } else if (line.contains(" WARN ")) {
                LOGGER.warn(" {}: {}", fullTableName, line);
            } else if (line.contains(" INFO ")) {
                LOGGER.info(" {}: {}", fullTableName, line);
            } else if (line.contains(" DEBUG ")) {
                LOGGER.debug(" {}: {}", fullTableName, line);
            } else if (line.contains(" TRACE ")) {
                LOGGER.trace(" {}: {}", fullTableName, line);
            }
        } else {
            LOGGER.debug(" {}: {}", fullTableName, line);
        }
    }

    /**
     * Task start
     *
     * @param schemaName Schema name
     * @param tableName  Table name
     */
    public void onTaskStart(String schemaName, String tableName) {
        String fullTableName = schemaName + "." + tableName;
        LOGGER.info("DataX task started for {}", fullTableName);
        updateProgress(fullTableName, 0, ProgressStatus.NOT_MIGRATED.getCode());
    }

    /**
     * Task complete
     *
     * @param schemaName   Schema name
     * @param tableName    Table name
     * @param success      Whether the task is successful
     * @param errorMessage Error message
     */
    public void onTaskComplete(String schemaName, String tableName, boolean success, String errorMessage) {
        String fullTableName = schemaName + "." + tableName;
        if (success) {
            LOGGER.info("DataX task completed successfully for {}", fullTableName);
            updateProgress(fullTableName, 100, ProgressStatus.MIGRATED_COMPLETE.getCode());
        } else {
            LOGGER.error("DataX task completed with error for {}: {}", fullTableName, errorMessage);
    
            updateProgress(fullTableName, 0, ProgressStatus.MIGRATED_FAILURE.getCode());
        }
        List<String> errors = errorCache.getOrDefault(fullTableName, new ArrayList<>());
        if (!errors.isEmpty()) {
            StringBuilder errorBuilder = new StringBuilder();
            errorBuilder.append(errorMessage).append("\n");
            for (String error : errors) {
                errorBuilder.append(error).append("\n");
            }
            MigrationErrorLogger.getInstance().logSqlError("DataX Task Has Some Errors", fullTableName,"DataX task execution",errorBuilder.toString());
            errorCache.remove(fullTableName);
        }
    }

    /**
     * Update progress
     *
     * @param tableName  Table name
     * @param progress   Progress
     * @param statusCode Status code
     */
    private void updateProgress(String tableName, int progress, int statusCode) {
        LOGGER.debug("Updated progress for {}: {}% (status: {})", tableName, progress, statusCode);
        if (!isDumpJson) {
            return;
        }
        ProgressTracker tracker = ProgressTracker.getInstance();
        ProgressInfo progressInfo = new ProgressInfo();
        progressInfo.setName(tableName);
        progressInfo.setPercent(progress);
        progressInfo.setStatus(statusCode);
        tracker.upgradeTableProgress(tableName, progressInfo);
    }
}