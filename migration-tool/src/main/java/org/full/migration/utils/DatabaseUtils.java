package org.full.migration.utils;

/**
 * FileUtils
 *
 * @since 2025-11-24
 */
public class DatabaseUtils {

    /**
     * Put double quotation marks around the object name
     *
     * @param name database object name
     *
     * @return project name with double quotation
     */
    public static String formatObjName(String name) {
        if (name == null) {
            return null;
        }
        if (name.startsWith("\"") && name.endsWith("\"") && name.length() >= 2) {
            return name;
        }
        return "\"" + name.replace("\"", "\"\"") + "\"";
    }

    /**
     * Put double quotation marks around the column list
     *
     * @param columnNames String the column names
     *
     * @return String the column names with double quotation marks
     */
    public static String formatMultiColName(String columnNames) {
        if (columnNames == null || columnNames.isEmpty()) {
            return columnNames;
        }
        String[] columnList = columnNames.split(",");
        StringBuilder builder = new StringBuilder();
        for (String column : columnList) {
            builder.append(formatObjName(column.trim())).append(",");
        }
        builder.deleteCharAt(builder.length() - 1);
        return builder.toString();
    }
}
