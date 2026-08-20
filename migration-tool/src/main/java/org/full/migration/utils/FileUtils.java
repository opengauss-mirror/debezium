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
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Comparator;
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
        if (file == null) {
            return;
        }
        Path filePath = file.toPath();
        try {
            if (isSymbolicLinkInvolved(filePath)) {
                LOGGER.warn("Refuse to write progress file [{}]: the path is or is contained in a symbolic link.",
                    filePath);
                return;
            }
            if (!Files.exists(filePath)) {
                return;
            }
            // Open for write without truncating, re-check that the path is still not a symbolic link,
            // and only then truncate and write through the already opened handle. The handle is bound
            // to the file resolved at open time, so a symbolic link pre-placed or swapped in before the
            // open can no longer redirect the truncation/write to an arbitrary file (CWE-59). Opening
            // without truncating avoids destroying an existing file before the re-check completes.
            try (FileChannel channel = FileChannel.open(filePath, StandardOpenOption.WRITE)) {
                if (Files.isSymbolicLink(filePath)) {
                    LOGGER.warn("Refuse to write progress file [{}]: it was replaced by a symbolic link.",
                        filePath);
                    return;
                }
                channel.truncate(0L);
                channel.write(ByteBuffer.wrap((jsonStr + System.lineSeparator()).getBytes(StandardCharsets.UTF_8)));
            }
        } catch (IOException exp) {
            LOGGER.warn(
                "IO exception occurred while writing progress to file, process or fail sql will not be committed",
                exp);
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
            Path filePath = Paths.get(path);
            if (isSymbolicLinkInvolved(filePath)) {
                LOGGER.warn("Refuse to initialize progress file [{}]: the path is or is contained in a symbolic link.",
                    path);
                return null;
            }
            processFile = new File(path);
            if (!processFile.exists()) {
                Files.createFile(filePath);
            }
        } catch (IOException exp) {
            LOGGER.warn("Failed to create directors, please check file path.", exp);
        }
        return processFile;
    }

    /**
     * Check whether the given path itself or any of its parent directories is a symbolic link.
     * Creating or truncating a progress file through a symbolic link would allow a lower privilege
     * user that can write the status directory to redirect the write to an arbitrary file the
     * migration process can modify (CWE-59). Every path component is checked, so a symbolic link
     * used as the status directory itself is rejected as well.
     *
     * @param path Path the path to check
     * @return Boolean true when the path itself or one of its parents is a symbolic link
     */
    private static boolean isSymbolicLinkInvolved(Path path) {
        for (Path current = path; current != null; current = current.getParent()) {
            if (Files.isSymbolicLink(current)) {
                return true;
            }
        }
        return false;
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
    public static BufferedWriter createNewFileWriter(Table table, String tableCsvPath, int fileIndex) {
        try {
            File csvFile = new File(getCurrentFilePath(table, tableCsvPath, fileIndex));

            if (!csvFile.exists()) {
                boolean fileCreated = csvFile.createNewFile();
                if (!fileCreated) {
                    LOGGER.error("Failed to create file: {}", csvFile.getAbsolutePath());
                    return null;
                }
            }

            modifyFilePermission(csvFile.toPath());
            return new BufferedWriter(
                    new OutputStreamWriter(Files.newOutputStream(csvFile.toPath()), StandardCharsets.UTF_8));

        } catch (IOException e) {
            LOGGER.error("Error creating file writer: {}", e.getMessage());
            return null;
        } catch (Exception e) {
            LOGGER.error("Unexpected error: {}", e.getMessage());
            return null;
        }
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
            filePath.toFile().setReadable(true);
            filePath.toFile().setWritable(true);
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
        return tableCsvPath + File.separator + String.format(Locale.ROOT, "%s_%s_%d.csv",
            sanitizeFileNameComponent(table.getSchemaName()), sanitizeFileNameComponent(table.getTableName()),
            fileIndex);
    }

    /**
     * Sanitizes a schema/table name used to build a CSV export file name so that it cannot contain
     * path separators or path traversal sequences (e.g. {@code ..\..\evil}), which would otherwise
     * allow the CSV file to be written or deleted at an arbitrary location outside the configured
     * export directory (CWE-22). All path separators are replaced with '_', so the resulting name is
     * always a single path component in which '..' can no longer escape the directory. Ordinary
     * names that contain neither '/' nor '\' are returned unchanged.
     */
    private static String sanitizeFileNameComponent(String name) {
        return name.replace('\\', '_').replace('/', '_');
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

    /**
     * deleteDir
     * Deletes a directory and all its contents recursively
     *
     * @param directoryPath Path of the directory to delete
     */
    public static void deleteDir(String directoryPath) {
        Path dirPath = Paths.get(directoryPath);
        if (!Files.exists(dirPath)) {
            LOGGER.debug("Directory does not exist: {}", directoryPath);
            return;
        }
        if (!Files.isDirectory(dirPath)) {
            LOGGER.warn("Path is not a directory: {}", directoryPath);
            return;
        }
        try {
            Files.walk(dirPath)
                    .sorted(Comparator.reverseOrder())
                    .forEach(path -> {
                        try {
                            Files.delete(path);
                            LOGGER.debug("Deleted: {}", path.toAbsolutePath());
                        } catch (IOException e) {
                            LOGGER.warn("Failed to delete: {}, error: {}", path.toAbsolutePath(), e.getMessage());
                        }
                    });
        } catch (IOException e) {
            LOGGER.error("Error walking directory: {}, error: {}", directoryPath, e.getMessage());
        }
    }

    /**
     * validatePath
     * Validates that a path is safe and within the specified base directory
     *
     * @param path Path to validate
     * @param baseDir Base directory to restrict to
     * @return True if the path is safe, false otherwise
     */
    public static boolean validatePath(String path, String baseDir) {
        try {
            Path normalizedPath = Paths.get(path).toAbsolutePath().normalize();
            Path normalizedBaseDir = Paths.get(baseDir).toAbsolutePath().normalize();
            return normalizedPath.startsWith(normalizedBaseDir);
        } catch (Exception e) {
            LOGGER.warn("Path validation failed: {}", e.getMessage());
            return false;
        }
    }

    /**
     * getSafePath
     * Gets a safe path within the specified base directory
     *
     * @param fileName File name to use
     * @param baseDir Base directory to restrict to
     * @return Safe path within the base directory
     */
    public static String getSafePath(String fileName, String baseDir) {
        // Remove any path separators from the file name to prevent directory traversal
        String safeFileName = fileName.replace(File.separator, "");
        // Create path within base directory
        return baseDir + File.separator + safeFileName;
    }

    /**
     * isPathWithinDirectory
     * Checks if a path is within the specified directory
     *
     * @param path Path to check
     * @param directory Directory to check against
     * @return True if the path is within the directory, false otherwise
     */
    public static boolean isPathWithinDirectory(String path, String directory) {
        try {
            Path normalizedPath = Paths.get(path).toAbsolutePath().normalize();
            Path normalizedDirectory = Paths.get(directory).toAbsolutePath().normalize();
            return normalizedPath.startsWith(normalizedDirectory);
        } catch (Exception e) {
            LOGGER.warn("Path check failed: {}", e.getMessage());
            return false;
        }
    }
}
