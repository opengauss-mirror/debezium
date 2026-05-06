/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */

package org.full.migration.datax;

import org.full.migration.exception.DataXMigrationException;
import org.full.migration.exception.ErrorCode;
import org.full.migration.utils.DataXConfigUtils;
import org.full.migration.datax.config.DataXCommonConfig;
import org.full.migration.datax.config.DataXConfigContext;
import org.full.migration.datax.config.DataXConfigFactory;
import org.full.migration.datax.config.DataXConfigStrategy;
import org.full.migration.datax.listener.DataXListener;
import org.full.migration.datax.model.DataXConfig;
import org.full.migration.utils.DataXJsonUtils;
import org.full.migration.exception.ConfigurationException;
import org.full.migration.model.config.GlobalConfig;
import org.full.migration.utils.FileUtils;
import org.full.migration.utils.MemoryUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * DataXManager
 * Manages the execution and configuration of DataX
 *
 * @since 2025-04-18
 */
public class DataXManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(DataXManager.class);
    private static final String DATAX_HOME = System.getProperty("user.dir") + File.separator + "datax";
    private static final AtomicInteger JOB_ID_COUNTER = new AtomicInteger(1);
    
    private final DataXCommonConfig commonConfig;
    private final DataXListener dataXListener;
    private final boolean enableKeepDataXTemporaryConfig;
    private final String temporaryConfigDir;
    private final ExecutorService executorService;

    /**
     * Constructor
     *
     * @param globalConfig Global configuration object
     */
    public DataXManager(GlobalConfig globalConfig) throws ConfigurationException {
        this.dataXListener = DataXListener.getInstance();
        this.dataXListener.setEnableOutputDataxLogs(globalConfig.getDatax().getEnableOutputDataxLogs());
        this.dataXListener.setIsDumpJson(globalConfig.getIsDumpJson());
        this.commonConfig = DataXConfigUtils.loadCommonConfig(globalConfig);
        this.enableKeepDataXTemporaryConfig = globalConfig.getDatax().getEnableKeepDataXTemporaryConfig();
        this.temporaryConfigDir = DATAX_HOME + File.separator + "temp" + File.separator + UUID.randomUUID().toString();
        this.executorService = Executors.newFixedThreadPool(globalConfig.getSourceConfig().getWriterNum());
        FileUtils.createDir(temporaryConfigDir);
        LOGGER.info("DataXManager initialized with temporary config dir: {}", temporaryConfigDir);
        LOGGER.info("DataXManager initialized with common config: {} ", commonConfig);
    }
    
    /**
     * Shutdown the executor service
     */
    public void shutdown() {
        if (executorService != null && !executorService.isShutdown()) {
            LOGGER.info("Shutting down thread pool...");
            executorService.shutdown();
            try {
                if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
                    LOGGER.warn("Thread pool did not terminate within 5 seconds, forcing shutdown");
                    executorService.shutdownNow();
                }
            } catch (InterruptedException e) {
                LOGGER.warn("Interrupted while shutting down thread pool", e);
                executorService.shutdownNow();
            }
            LOGGER.info("Thread pool shutdown completed");
        }
    }

    /**
     * <pre> Executes the full data migration using DataX.
     * Before executing the migration datax task, it check if there is enough memory to execute the DataX task
     * </pre>
     * @param context DataX configuration context object
     * @return Whether the migration was successful
     * @throws DataXMigrationException If an error occurs during migration execution
     */
    public boolean executeMigration(DataXConfigContext context) throws DataXMigrationException {
        String schemaName = context.getSchemaName();
        String tableName = context.getTableName();
        String migrationTaskName = schemaName + "_" + tableName + "_" + System.currentTimeMillis();
        LOGGER.info("Executing full migration for table: {}", migrationTaskName);
        DataXConfigUtils.prepareCommonConfig(commonConfig, context.getSourceConfig(), context.getTargetConfig());
        context.setCommonConfig(commonConfig);
        DataXConfigResult configResult = null;
        try {
            configResult = generateDataXConfig(context, migrationTaskName);
            MemoryUtils.checkMemoryAvailability(configResult.getJvmParameters(), schemaName + "." + tableName);
            dataXListener.onTaskStart(schemaName, tableName);
            boolean isSuccess = runDataXWithPyScript(configResult, schemaName, tableName, migrationTaskName);
            LOGGER.info("Full migration for {} completed with status: {}", migrationTaskName, isSuccess);
            dataXListener.onTaskComplete(schemaName, tableName, isSuccess, isSuccess ? null : "Task execution failed");
            return isSuccess;
        } catch (DataXMigrationException e) {
            LOGGER.error("Error executing full migration for {}: {}", migrationTaskName, e.getMessage());
            dataXListener.onTaskComplete(schemaName, tableName, false, e.getMessage());
            throw e;
        } finally {
            if (!enableKeepDataXTemporaryConfig && configResult != null) {
                String configFile = configResult.getConfigFile();
                if (FileUtils.isPathWithinDirectory(configFile, temporaryConfigDir)) {
                    File tempFile = new File(configFile);
                    if (tempFile.exists() && !tempFile.delete()) {
                        LOGGER.warn("Failed to delete temporary DataX config file: {}", configFile);
                    }
                } else {
                    LOGGER.warn("Config file path validation failed, skipping deletion: {}", configFile);
                }
            }
        }
    }

    /**
     * generateDataXConfig
     * Generates a DataX configuration file
     *
     * @param context           DataX configuration context object
     * @param migrationTaskName datax migration task name
     * @return DataXConfigResult containing configuration file path and JVM parameters
     * @throws DataXMigrationException IO exception
     */
    private DataXConfigResult generateDataXConfig(DataXConfigContext context, String migrationTaskName)
            throws DataXMigrationException {
        String configFileName = "datax_config_" + migrationTaskName + ".json";
        String configFile = FileUtils.getSafePath(configFileName, temporaryConfigDir);
        DataXConfigFactory factory = DataXConfigFactory.getInstance();
        DataXConfigStrategy strategy = factory.getApplicableStrategy(context);
        LOGGER.info("Using strategy: {} for table: {}.{} to generate DataX config {}", strategy.getStrategyName(),
                context.getSchemaName(), context.getTableName(), configFileName);
        DataXConfig dataXConfig = strategy.generateConfig(context);
        String configJson = DataXJsonUtils.toJson(dataXConfig);
        try (OutputStreamWriter writer = new OutputStreamWriter(new FileOutputStream(configFile),
                StandardCharsets.UTF_8)) {
            writer.write(configJson);
        } catch (IOException e) {
            throw new DataXMigrationException(ErrorCode.DATAX_CONFIG_ERROR.getCode(),
                    "Failed to generate DataX config file " + configFileName, e);
        }
        long rowCount = context.getTable().getEstimatedRowCount();
        LOGGER.debug("Generated DataX config file: {}, JVM parameters: {}", configFile, strategy.getJvmParameters(rowCount));
        return new DataXConfigResult(configFile, strategy.getJvmParameters(rowCount));
    }

    /**
     * runDataXWithPyScript
     * Run DataX task using datax.py script
     *
     * @param configResult      DataXConfigResult containing configuration file path and JVM parameters
     * @param schemaName        Schema name
     * @param tableName         Table name
     * @param migrationTaskName migrationTaskName
     * @return Whether the execution is successful
     * @throws DataXMigrationException DataX execution exception
     */
    private boolean runDataXWithPyScript(DataXConfigResult configResult, String schemaName, String tableName,
                                         String migrationTaskName) throws DataXMigrationException {
        Process process = null;
        OutputStreamWriter logWriter = null;
        try {
            List<String> commandList = buildCommandList(configResult);
            String logFile = createLogFile(migrationTaskName);
            logWriter = new OutputStreamWriter(new FileOutputStream(logFile), StandardCharsets.UTF_8);
            process = startDataXProcess(commandList);
            startOutputCaptureThread(process, logWriter);
            return processDataXOutput(logFile, process, schemaName, tableName);
        } catch (IOException e) {
            handleExecutionException(process, e, "IO异常");
        } catch (InterruptedException e) {
            handleInterruptedException(process, e);
        } finally {
            closeLogWriter(logWriter);
        }
        return false;
    }

    /**
     * 启动输出捕获线程
     */
    private void startOutputCaptureThread(Process process, OutputStreamWriter logWriter) {
        final OutputStreamWriter finalLogWriter = logWriter;
        final Process finalProcess = process;
        executorService.submit(() -> {
            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(finalProcess.getInputStream(), StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    finalLogWriter.write(line);
                    finalLogWriter.write(System.lineSeparator());
                    finalLogWriter.flush();
                }
            } catch (Exception e) {
                LOGGER.warn("捕获进程输出时出错: {}", e.getMessage());
            }
        });
    }



    /**
     * 处理执行异常
     */
    private void handleExecutionException(Process process, IOException e, String errorType) throws DataXMigrationException {
        if (process != null) {
            DataXInstall.removeProcess(process);
            LOGGER.debug("由于{}从运行列表中移除DataX进程, PID: {}", errorType, process.pid());
        }
        throw new DataXMigrationException(ErrorCode.DATAX_EXECUTION_FAILED.getCode(),
                "DataX执行失败: " + e.getMessage(), e);
    }

    /**
     * 处理中断异常
     */
    private void handleInterruptedException(Process process, InterruptedException e) throws DataXMigrationException {
        if (process != null) {
            DataXInstall.removeProcess(process);
            LOGGER.debug("由于中断异常从运行列表中移除DataX进程, PID: {}", process.pid());
        }
        Thread.currentThread().interrupt();
        throw new DataXMigrationException(ErrorCode.DATAX_EXECUTION_FAILED.getCode(),
                "DataX执行被中断", e.getMessage(), e);
    }

    /**
     * 关闭日志写入器
     */
    private void closeLogWriter(OutputStreamWriter logWriter) {
        if (logWriter != null) {
            try {
                logWriter.close();
            } catch (IOException e) {
                LOGGER.warn("关闭日志写入器失败", e);
            }
        }
    }

    /**
     * Build the command list for DataX execution
     *
     * @param DataXConfigResult        configResult
     * @return Command list
     */
    private List<String> buildCommandList(DataXConfigResult configResult) {
        int jobId = JOB_ID_COUNTER.addAndGet(1);
        return DataXCommandWrapper.buildCommandList(configResult, jobId);
    }

    /**
     * Create a log file for DataX output
     *
     * @param migrationTaskName Migration task name
     * @return Log file path
     */
    private String createLogFile(String migrationTaskName) {
        String logFileName = "datax_log_" + migrationTaskName + ".log";
        return FileUtils.getSafePath(logFileName, temporaryConfigDir);
    }

    /**
     * Start the DataX process, Use the command list directly for all platforms
     *
     * @param commandList Command list
     * @return Process object
     * @throws DataXMigrationException If an datax tassk start error occurs
     */
    private Process startDataXProcess(List<String> commandList) throws DataXMigrationException {
        try {
            Process process = DataXCommandWrapper.executeCommand(commandList);
            DataXInstall.addProcess(process);
            LOGGER.debug("Added DataX process to running list, PID: {}", process.pid());
            return process;
        } catch (IOException e) {
            throw new DataXMigrationException(ErrorCode.DATAX_EXECUTION_FAILED.getCode(),
                    "start datax command error, by param: " + commandList);
        }
    }

    /**
     * Process DataX output by monitoring the log file
     *
     * @param logFile    Log file path
     * @param process    DataX process
     * @param schemaName Schema name
     * @param tableName  Table name
     * @return Whether the execution is successful
     * @throws IOException          If an I/O error occurs
     * @throws InterruptedException If the process is interrupted
     * @throws DataXMigrationException If an datax tassk start error occurs
     */
    private boolean processDataXOutput(String logFile, Process process, String schemaName, String tableName)
            throws IOException, InterruptedException, DataXMigrationException {
        AtomicBoolean success = new AtomicBoolean(false);
        StringBuilder errorBuilder = new StringBuilder();

        LogMonitoringContext context = new LogMonitoringContext(logFile, process, schemaName, tableName, success, errorBuilder);
        Thread logMonitorThread = startLogMonitorThread(context);
        int exitCode = process.waitFor();
        logMonitorThread.join(5000); 
        DataXInstall.removeProcess(process);
        LOGGER.debug("Removed DataX process from running list, PID: {}, exit code: {}", process.pid(), exitCode);

        if (exitCode != 0) {
            String errorMessage = errorBuilder.toString();
            if (!errorMessage.isEmpty()) {
                LOGGER.error("DataX process exited with error:");
                LOGGER.error(errorMessage);
            }
        }

        return exitCode == 0 && success.get();
    }
    
    /**
     * Start a thread to monitor the log file
     *
     * @param context Log monitoring context
     * @return Log monitor thread
     */
    private Thread startLogMonitorThread(final LogMonitoringContext context) {
        Thread logMonitorThread = new Thread(() -> {
            try {
                monitorLogFile(context);
            } catch (Exception e) {
                LOGGER.warn("Error monitoring DataX log file: {}", e.getMessage());
            }
        });
        logMonitorThread.start();
        return logMonitorThread;
    }
    
    /**
     * Monitor the log file for DataX output
     *
     * @param context Log monitoring context
     * @throws Exception If an error occurs
     */
    private void monitorLogFile(LogMonitoringContext context) throws Exception {
        File log = new File(context.getLogFile());
        long lastReadPosition = 0;
        java.util.regex.Pattern logLinePattern = java.util.regex.Pattern
                .compile("^\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d{3} \\[.*\\] (INFO|ERROR|WARN|DEBUG|TRACE)");
        
        // Monitor log file while process is alive
        while (context.getProcess().isAlive()) {
            if (log.exists() && log.length() > lastReadPosition) {
                lastReadPosition = processLogFileSection(log, lastReadPosition, logLinePattern, context);
            }
            Thread.sleep(100); // Small delay to avoid busy polling
        }
        
        // Process any remaining content in the log file
        if (log.exists()) {
            processLogFileSection(log, lastReadPosition, logLinePattern, context);
        }
    }
    
    /**
     * Process a section of the log file
     *
     * @param log          Log file
     * @param lastReadPosition Last read position
     * @param logLinePattern Log line pattern
     * @param context      Log monitoring context
     * @return New last read position
     * @throws IOException If an I/O error occurs
     */
    private long processLogFileSection(File log, long lastReadPosition, java.util.regex.Pattern logLinePattern,
                                      LogMonitoringContext context) throws IOException {
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(new FileInputStream(log), StandardCharsets.UTF_8))) {
            reader.skip(lastReadPosition);
            String line;
            StringBuilder logBuffer = new StringBuilder();
            
            while ((line = reader.readLine()) != null) {
                lastReadPosition += line.length() + System.lineSeparator().length();
                
                if (line.contains("Exception") || line.contains("Error") || line.contains("ERROR")
                        || line.contains("exception")) {
                    context.getErrorBuilder().append(line).append(System.lineSeparator());
                }

                // Check for opengauss login failure error
                if (line.contains("login database failed")) {
                    context.getProcess().destroy(); // Terminate the process
                    return lastReadPosition;
                }

                boolean isNewLogLine = logLinePattern.matcher(line).find();
                if (isNewLogLine) {
                    processBufferedLog(logBuffer, context.getSchemaName(), context.getTableName(), 
                            context.getErrorBuilder(), context.getSuccess());
                    processCurrentLine(line, context.getSchemaName(), context.getTableName(), context.getSuccess());
                } else {
                    appendToBuffer(logBuffer, line, context.getErrorBuilder(), context.getSuccess());
                }
            }
            processBufferedLog(logBuffer, context.getSchemaName(), context.getTableName(), 
                    context.getErrorBuilder(), context.getSuccess());
        }
        return lastReadPosition;
    }

    private void processBufferedLog(StringBuilder logBuffer, String schemaName, String tableName,
                                    StringBuilder errorBuilder, AtomicBoolean success) {
        if (logBuffer.length() > 0) {
            String bufferedLog = logBuffer.toString();
            dataXListener.processOutputLine(bufferedLog, schemaName, tableName);
            errorBuilder.append(bufferedLog).append(System.lineSeparator());
            if (bufferedLog.contains("任务启动时刻") || bufferedLog.contains("任务结束时刻") || bufferedLog.contains("任务总计耗时")) {
                success.set(true);
            } else if (bufferedLog.contains("Exception") || bufferedLog.contains("Error")) {
                success.set(false);
            }
            logBuffer.setLength(0);
        }
    }

    private void processCurrentLine(String line, String schemaName, String tableName, AtomicBoolean success) {
        dataXListener.processOutputLine(line, schemaName, tableName);
        if (line.contains("任务启动时刻") || line.contains("任务结束时刻") || line.contains("任务总计耗时")) {
            success.set(true);
        } else if (line.contains("Exception") || line.contains("Error")) {
            success.set(false);
        }
    }

    private void appendToBuffer(StringBuilder logBuffer, String line, StringBuilder errorBuilder,
                                AtomicBoolean success) {
        if (logBuffer.length() > 0) {
            logBuffer.append(System.lineSeparator());
        }
        logBuffer.append(line);
        if (line.contains("Exception") || line.contains("Error")) {
            success.set(false);
            errorBuilder.append(line).append(System.lineSeparator());
        }
    }
}
