/*
 * Copyright (c) 2025-2025 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *           http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package org.full.migration.coordinator;

import com.alibaba.fastjson.JSON;

import org.apache.commons.lang3.StringUtils;
import org.full.migration.model.FullName;
import org.full.migration.model.TaskTypeEnum;
import org.full.migration.model.progress.MigrationProgressInfo;
import org.full.migration.model.progress.ProgressInfo;
import org.full.migration.model.progress.ProgressStatus;
import org.full.migration.model.progress.TotalInfo;
import org.full.migration.utils.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * ProgressTracker
 *
 * @since 2025-04-18
 */
public class ProgressTracker {
    private static final Logger LOGGER = LoggerFactory.getLogger(ProgressTracker.class);
    private static final int COMMIT_TIME_INTERVAL = 2;
    private static final int TIME_UNIT = 1000;
    private static final int MEMORY_UNIT = 1024;
    private static ProgressTracker instance;

    private final Map<String, ProgressInfo> progressMap;
    private final AtomicBoolean isTaskStop;
    private final long startTime;
    private final ScheduledExecutorService fullProgressReportService;
    private final String path;
    private final File file;

    private final AtomicLong extractRows = new AtomicLong(0L);
    private final AtomicLong csvRows = new AtomicLong(0L);
    private final AtomicLong loadRows = new AtomicLong(0L);
    private long lastExtractRows;
    private long lastCsvRows;
    private long lastLoadRows;
    private long lastStageTime;
    private long extractSpeed;
    private long csvSpeed;
    private long loadSpeed;

    private ProgressTracker(String path, String taskType) {
        this.progressMap = new ConcurrentHashMap<>();
        this.isTaskStop = new AtomicBoolean(false);
        this.startTime = System.currentTimeMillis();
        fullProgressReportService = Executors.newSingleThreadScheduledExecutor(
            (r) -> new Thread(r, "fullProgressReportThread"));
        File dir = new File(path);
        if (!dir.exists()) {
            dir.mkdirs();  // 创建目录
        }
        this.path = path;
        this.file = FileUtils.initFile(this.path + File.separator + taskType + ".json");
    }

    /**
     * initInstance
     *
     * @param path path
     * @param taskType taskType
     */
    public static synchronized void initInstance(String path, String taskType) {
        if (instance == null) {
            instance = new ProgressTracker(path, taskType);
        }
    }

    /**
     * getInstance
     *
     * @return instance
     */
    public static synchronized ProgressTracker getInstance() {
        if (instance == null) {
            throw new IllegalStateException("Singleton must be initialized first with getInstance(String, String)");
        }
        return instance;
    }

    /**
     * recordTableProgress
     */
    public void recordTableProgress() {
        fullProgressReportService.scheduleAtFixedRate(this::reportTableProgress, COMMIT_TIME_INTERVAL,
            COMMIT_TIME_INTERVAL, TimeUnit.SECONDS);
    }

    /**
     * recordObjectProgress
     *
     * @param objectType objectType
     */
    public void recordObjectProgress(TaskTypeEnum objectType) {
        fullProgressReportService.scheduleAtFixedRate(() -> this.reportObjectProgress(objectType), COMMIT_TIME_INTERVAL,
            COMMIT_TIME_INTERVAL, TimeUnit.SECONDS);
    }

    /**
     * recordKeyAndIndexProgress
     *
     * @param keyAndIndexType keyAndIndexType
     */
    public void recordKeyAndIndexProgress(TaskTypeEnum keyAndIndexType) {
        fullProgressReportService.scheduleAtFixedRate(() -> this.reportKeyAndIndexProgress(keyAndIndexType), COMMIT_TIME_INTERVAL,
                COMMIT_TIME_INTERVAL, TimeUnit.SECONDS);
    }

    private void reportKeyAndIndexProgress(TaskTypeEnum keyAndIndexType) {
        doReportKeyAndIndexProgress(keyAndIndexType);
        if (isTaskStop.get()) {
            fullProgressReportService.shutdown();
            doReportKeyAndIndexProgress(keyAndIndexType);
            LOGGER.info("{} migration complete. full report thread is close.", keyAndIndexType.getTaskType());
        }
    }

    private void doReportKeyAndIndexProgress(TaskTypeEnum keyAndIndexType) {
        MigrationProgressInfo migrationProgressInfo = new MigrationProgressInfo();
        for (Map.Entry<String, ProgressInfo> tableProgress : progressMap.entrySet()) {
            ProgressInfo progressInfo = tableProgress.getValue();
            migrationProgressInfo.addKeyAndIndex(progressInfo, keyAndIndexType);
        }
        FileUtils.writeToFile(file, JSON.toJSONString(migrationProgressInfo));
    }

    private void reportObjectProgress(TaskTypeEnum objectType) {
        doReportObjectProgress(objectType);
        if (isTaskStop.get()) {
            fullProgressReportService.shutdown();
            doReportObjectProgress(objectType);
            LOGGER.info("{} migration complete. full report thread is close.", objectType.getTaskType());
        }
    }

    private void doReportObjectProgress(TaskTypeEnum objectType) {
        MigrationProgressInfo migrationProgressInfo = new MigrationProgressInfo();
        for (Map.Entry<String, ProgressInfo> tableProgress : progressMap.entrySet()) {
            ProgressInfo progressInfo = tableProgress.getValue();
            migrationProgressInfo.addObject(progressInfo, objectType);
        }
        FileUtils.writeToFile(file, JSON.toJSONString(migrationProgressInfo));
    }

    private void reportTableProgress() {
        updateStageSpeeds();
        doReportTableProgress();
        if (isTaskStop.get()) {
            fullProgressReportService.shutdown();
            doReportTableProgress();
            LOGGER.info("table migration complete. full report thread is close.");
        }
    }

    private void doReportTableProgress() {
        MigrationProgressInfo migrationProgressInfo = new MigrationProgressInfo();
        long totalRecord = 0L;
        double totalData = 0;
        for (Map.Entry<String, ProgressInfo> tableProgress : progressMap.entrySet()) {
            ProgressInfo progressInfo = tableProgress.getValue();
            if (progressInfo.getStatus() == ProgressStatus.IN_MIGRATED.getCode()
                    && Float.compare(progressInfo.getPercent(), 1.0f) == 0) {
                progressInfo.setStatus(ProgressStatus.MIGRATED_COMPLETE.getCode());
            }
            migrationProgressInfo.addTable(progressInfo);
            totalData += progressInfo.getData();
            totalRecord += progressInfo.getRecord();
        }
        long currentTime = System.currentTimeMillis();
        int timeInterval = (int) ((currentTime - startTime) / TIME_UNIT);
        BigDecimal speed = new BigDecimal(totalData / (timeInterval * MEMORY_UNIT));
        migrationProgressInfo.setTotal(
                new TotalInfo(totalRecord, getFormatDouble(new BigDecimal(totalData / MEMORY_UNIT ), 2),
                        timeInterval, getFormatDouble(speed, 2),
                        extractSpeed, csvSpeed, loadSpeed));
        FileUtils.writeToFile(file, JSON.toJSONString(migrationProgressInfo));
    }

    /**
     * updateStageSpeeds
     * Calculate per-stage speed over the window since the last refresh and keep
     * the result in fields, so the final report reuses the last values instead
     * of computing a near-zero-width window.
     */
    private void updateStageSpeeds() {
        long curExtractRows = extractRows.get();
        long curCsvRows = csvRows.get();
        long curLoadRows = loadRows.get();
        long currentTime = System.currentTimeMillis();
        if (lastStageTime > 0 && currentTime > lastStageTime) {
            double seconds = (currentTime - lastStageTime) / (double) TIME_UNIT;
            extractSpeed = Math.round((curExtractRows - lastExtractRows) / seconds);
            csvSpeed = Math.round((curCsvRows - lastCsvRows) / seconds);
            loadSpeed = Math.round((curLoadRows - lastLoadRows) / seconds);
        }
        lastExtractRows = curExtractRows;
        lastCsvRows = curCsvRows;
        lastLoadRows = curLoadRows;
        lastStageTime = currentTime;
    }

    /**
     * recordExtractRows
     *
     * @param rows rows
     */
    public static void recordExtractRows(long rows) {
        ProgressTracker tracker = instance;
        if (tracker != null) {
            tracker.extractRows.addAndGet(rows);
        }
    }

    /**
     * recordCsvRows
     *
     * @param rows rows
     */
    public static void recordCsvRows(long rows) {
        ProgressTracker tracker = instance;
        if (tracker != null) {
            tracker.csvRows.addAndGet(rows);
        }
    }

    /**
     * recordLoadRows
     *
     * @param rows rows
     */
    public static void recordLoadRows(long rows) {
        ProgressTracker tracker = instance;
        if (tracker != null) {
            tracker.loadRows.addAndGet(rows);
        }
    }

    private double getFormatDouble(BigDecimal decimal, int precision) {
        return decimal.setScale(precision, RoundingMode.HALF_UP).doubleValue();
    }

    /**
     * putProgressMap
     *
     * @param schema schema
     * @param name name
     */
    public void putProgressMap(String schema, String name) {
        ProgressInfo progressInfo = new ProgressInfo();
        progressMap.put(String.format("%s.%s", schema, name), progressInfo);
    }

    /**
     * upgradeTableProgress
     *
     * @param fullName fullName
     * @param progressInfo progressInfo
     */
    public void upgradeTableProgress(String fullName, ProgressInfo progressInfo) {
        ProgressInfo preProgressInfo = progressMap.get(fullName);
        if (preProgressInfo.getPercent() < progressInfo.getPercent() || preProgressInfo.getName() == null) {
            if (preProgressInfo.getStatus() == ProgressStatus.MIGRATED_FAILURE.getCode()) {
                progressInfo.setStatus(ProgressStatus.MIGRATED_FAILURE.getCode());
                progressInfo.setError(preProgressInfo.getError() + File.separator + progressInfo.getError());
            }
            progressInfo.setPercent(
                progressInfo.getPercent() > 1 ? preProgressInfo.getPercent() : progressInfo.getPercent());
            progressInfo.setRecord(preProgressInfo.getRecord() + progressInfo.getRecord());
            progressMap.put(fullName, progressInfo);
        }
    }

    /**
     * upgradeObjectProgressMap
     *
     * @param fullName fullName
     * @param status status
     * @param errMsg errMsg
     */
    public void upgradeObjectProgressMap(FullName fullName, ProgressStatus status, String errMsg) {
        if (!progressMap.containsKey(fullName.getFullName())) {
            return;
        }
        ProgressInfo progressInfo = progressMap.get(fullName.getFullName());
        progressInfo.setSchema(fullName.getSchema());
        progressInfo.setName(fullName.getName());
        progressInfo.setStatus(status.getCode());
        if (status == ProgressStatus.MIGRATED_COMPLETE || status == ProgressStatus.MIGRATED_FAILURE) {
            progressInfo.setPercent(1);
        }
        if (StringUtils.isNotEmpty(errMsg)) {
            progressInfo.setError(errMsg);
        }
        progressMap.put(fullName.getFullName(), progressInfo);
    }

    /**
     * upgradeObjectProgressMap
     *
     * @param name name
     * @param status status
     * @param errMsg errMsg
     */
    public void upgradeKeyAndIndexProgressMap(String name, ProgressStatus status, String errMsg) {
        if (!progressMap.containsKey(name)) {
            return;
        }
        ProgressInfo progressInfo = progressMap.get(name);
        progressInfo.setStatus(status.getCode());
        if (status == ProgressStatus.MIGRATED_COMPLETE || status == ProgressStatus.MIGRATED_FAILURE) {
            progressInfo.setPercent(1);
        }
        if (StringUtils.isNotEmpty(errMsg)) {
            progressInfo.setError(errMsg);
        }
        progressMap.put(name, progressInfo);
    }

    /**
     * setIsTaskStop
     *
     * @param isTaskStop isTaskStop
     */
    public void setIsTaskStop(boolean isTaskStop) {
        this.isTaskStop.set(isTaskStop);
    }
}
