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
        if (name != null && !name.startsWith("\"")) {
            return "\"" + name + "\"";
        }
        return name;
    }
}
