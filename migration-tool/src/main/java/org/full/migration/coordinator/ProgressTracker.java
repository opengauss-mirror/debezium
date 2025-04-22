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

    private ProgressTracker(String path, String taskType) {
        this.progressMap = new ConcurrentHashMap<>();
        this.isTaskStop = new AtomicBoolean(false);
        this.startTime = System.currentTimeMillis();
        fullProgressReportService = Executors.newSingleThreadScheduledExecutor(
            (r) -> new Thread(r, "fullProgressReportThread"));
        this.path = new File(path).getParent();
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

    private void reportObjectProgress(TaskTypeEnum objectType) {
        MigrationProgressInfo migrationProgressInfo = new MigrationProgressInfo();
        for (Map.Entry<String, ProgressInfo> tableProgress : progressMap.entrySet()) {
            ProgressInfo progressInfo = tableProgress.getValue();
            migrationProgressInfo.addObject(progressInfo, objectType);
        }
        FileUtils.writeToFile(file, JSON.toJSONString(migrationProgressInfo));
        if (isTaskStop.get()) {
            fullProgressReportService.shutdown();
            LOGGER.info("full data migration complete. full report thread is close.");
        }
    }

    private void reportTableProgress() {
        MigrationProgressInfo migrationProgressInfo = new MigrationProgressInfo();
        long totalRecord = 0L;
        double totalData = 0;
        for (Map.Entry<String, ProgressInfo> tableProgress : progressMap.entrySet()) {
            ProgressInfo progressInfo = tableProgress.getValue();
            migrationProgressInfo.addTable(progressInfo);
            totalData += progressInfo.getData();
            totalRecord += progressInfo.getRecord();
        }
        long currentTime = System.currentTimeMillis();
        int timeInterval = (int) ((currentTime - startTime) / TIME_UNIT);
        BigDecimal speed = new BigDecimal(totalData / (timeInterval * MEMORY_UNIT * MEMORY_UNIT));
        migrationProgressInfo.setTotal(
            new TotalInfo(totalRecord, getFormatDouble(new BigDecimal(totalData / (MEMORY_UNIT * MEMORY_UNIT)), 2),
                timeInterval, getFormatDouble(speed, 2)));
        FileUtils.writeToFile(file, JSON.toJSONString(migrationProgressInfo));
        if (isTaskStop.get()) {
            fullProgressReportService.shutdown();
            LOGGER.info("full data migration complete. full report thread is close.");
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
        progressMap.put(String.format("%s.%s", schema, name), new ProgressInfo());
    }

    /**
     * upgradeTableProgress
     *
     * @param name name
     * @param progressInfo progressInfo
     */
    public void upgradeTableProgress(String name, ProgressInfo progressInfo) {
        ProgressInfo preProgressInfo = progressMap.get(name);
        if (preProgressInfo.getPercent() < progressInfo.getPercent()) {
            if (preProgressInfo.getStatus() == ProgressStatus.MIGRATED_FAILURE.getCode()) {
                progressInfo.setStatus(ProgressStatus.MIGRATED_FAILURE.getCode());
                progressInfo.setError(preProgressInfo.getError() + File.separator + preProgressInfo.getError());
            }
            progressInfo.setPercent(
                progressInfo.getPercent() > 1 ? preProgressInfo.getPercent() : progressInfo.getPercent());
            progressInfo.setRecord(preProgressInfo.getRecord() + progressInfo.getRecord());
            progressMap.put(name, progressInfo);
        }
    }

    /**
     * upgradeObjectProgressMap
     *
     * @param name name
     * @param status status
     */
    public void upgradeObjectProgressMap(String name, ProgressStatus status) {
        if (!progressMap.containsKey(name)) {
            return;
        }
        ProgressInfo progressInfo = progressMap.get(name);
        progressInfo.setStatus(status.getCode());
        if (status == ProgressStatus.MIGRATED_COMPLETE || status == ProgressStatus.MIGRATED_FAILURE) {
            progressInfo.setPercent(1);
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
