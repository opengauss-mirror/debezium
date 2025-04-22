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

package org.full.migration.utils;

import org.full.migration.model.table.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Locale;
import java.util.Set;

/**
 * FileUtils
 *
 * @since 2025-04-18
 */
public class FileUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(FileUtils.class);

    /**
     * writeToFile
     *
     * @param file file
     * @param jsonStr jsonStr
     */
    public static void writeToFile(File file, String jsonStr) {
        if (file.exists()) {
            try (FileWriter fileWriter = new FileWriter(file, false)) {
                fileWriter.write(jsonStr + System.lineSeparator());
            } catch (IOException exp) {
                LOGGER.warn(
                    "IO exception occurred while writing progress to file, process or fail sql will not be committed",
                    exp);
            }
        }
    }

    /**
     * initFile
     *
     * @param path path
     * @return File
     */
    public static File initFile(String path) {
        File processFile = null;
        try {
            processFile = new File(path);
            if (!processFile.exists()) {
                Files.createFile(Paths.get(path));
            }
        } catch (IOException exp) {
            LOGGER.warn("Failed to create directors, please check file path.", exp);
        }
        return processFile;
    }

    /**
     * createDir
     *
     * @param path path
     */
    public static void createDir(String path) {
        try {
            Path dirPath = Paths.get(path);
            Files.createDirectories(dirPath);
            modifyDirPermission(dirPath);
            LOGGER.info("success to create scv dir: {}", dirPath.toAbsolutePath());
        } catch (IOException e) {
            LOGGER.error("failed to create scv dir: {}, error message:{}", path, e.getMessage());
        }
    }

    /**
     * createNewFileWriter
     *
     * @param table table
     * @param tableCsvPath tableCsvPath
     * @param fileIndex fileIndex
     * @return BufferedWriter
     * @throws IOException IOException
     */
    public static BufferedWriter createNewFileWriter(Table table, String tableCsvPath, int fileIndex)
        throws IOException {
        File csvFile = new File(getCurrentFilePath(table, tableCsvPath, fileIndex));
        modifyFilePermission(csvFile.toPath());
        return new BufferedWriter(
            new OutputStreamWriter(Files.newOutputStream(csvFile.toPath()), StandardCharsets.UTF_8));
    }

    /**
     * set file permission 660
     *
     * @param filePath filePath
     * @throws IOException IOException
     */
    public static void modifyFilePermission(Path filePath) throws IOException {
        try {
            Set<PosixFilePermission> perms = Set.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE,
                PosixFilePermission.GROUP_READ);
            Files.setPosixFilePermissions(filePath, perms);
        } catch (UnsupportedOperationException e) {
            filePath.toFile().setReadable(true, true);
            filePath.toFile().setWritable(true, true);
            filePath.toFile().setReadable(true, false);
            filePath.toFile().setWritable(false, false);
            filePath.toFile().setExecutable(false, false);
        }
    }

    /**
     * set directory permission 750
     *
     * @param path path
     * @throws IOException IOException
     */
    public static void modifyDirPermission(Path path) throws IOException {
        try {
            Set<PosixFilePermission> perms = Set.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE,
                PosixFilePermission.OWNER_EXECUTE, PosixFilePermission.GROUP_READ, PosixFilePermission.GROUP_EXECUTE);
            Files.setPosixFilePermissions(path, perms);
        } catch (UnsupportedOperationException e) {
            File dir = path.toFile();
            dir.setReadable(true, true);
            dir.setWritable(true, true);
            dir.setExecutable(true, true);
            dir.setReadable(true, false);
            dir.setExecutable(true, false);
            dir.setWritable(false, false);
        }
    }

    /**
     * getCurrentFilePath
     *
     * @param table table
     * @param tableCsvPath tableCsvPath
     * @param fileIndex fileIndex
     * @return path of current file
     */
    public static String getCurrentFilePath(Table table, String tableCsvPath, int fileIndex) {
        return tableCsvPath + File.separator + String.format(Locale.ROOT, "%s_%s_%d.csv", table.getSchemaName(),
            table.getTableName(), fileIndex);
    }

    /**
     * clearCsvFile
     *
     * @param path path
     * @param isDeleteCsv isDeleteCsv
     */
    public static void clearCsvFile(String path, boolean isDeleteCsv) {
        if (!isDeleteCsv) {
            return;
        }
        Path filePath = Paths.get(path);
        LOGGER.debug("ready to delete csv file:{}", path);
        try {
            Files.deleteIfExists(filePath);
        } catch (IOException e) {
            LOGGER.error("clear csv file failure, file path:{}, error message:{}.", path, e.getMessage());
        }
    }
}
