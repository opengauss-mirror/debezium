package org.full.migration.translator;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * PostgresqlFuncTranslator
 *
 * @since 2025-05-19
 */
public class PostgresqlFuncTranslator {
    // 预编译所有正则表达式模式
    private static final Map<String, Pattern> PATTERNS = new HashMap<>();

    static {
        // 列名引用转换 [column] -> "column"
        PATTERNS.put("COLUMN_REF", Pattern.compile("\\[(\\w+)\\]"));
        // 日期时间函数
        PATTERNS.put("GETDATE", Pattern.compile("(?i)GETDATE\\(\\)"));
        PATTERNS.put("GETUTCDATE", Pattern.compile("(?i)GETUTCDATE\\(\\)"));
        PATTERNS.put("DATEADD", Pattern.compile("(?i)DATEADD\\((\\w+)\\s*,\\s*(\\d+)\\s*,\\s*([^)]+)\\)"));
        PATTERNS.put("DATEDIFF", Pattern.compile("(?i)DATEDIFF\\((\\w+)\\s*,\\s*([^,]+)\\s*,\\s*([^)]+)\\)"));
        PATTERNS.put("DATEPART", Pattern.compile("(?i)DATEPART\\((\\w+)\\s*,\\s*([^)]+)\\)"));
        // 字符串函数
        PATTERNS.put("LEN", Pattern.compile("(?i)LEN\\(([^)]+)\\)"));
        PATTERNS.put("CHARINDEX", Pattern.compile("(?i)CHARINDEX\\(([^,]+)\\s*,\\s*([^)]+)\\)"));
        PATTERNS.put("SUBSTRING", Pattern.compile("(?i)SUBSTRING\\(([^,]+)\\s*,\\s*([^,]+)\\s*,\\s*([^)]+)\\)"));
        // 其他函数
        PATTERNS.put("ISNULL", Pattern.compile("(?i)ISNULL\\(([^,]+)\\s*,\\s*([^)]+)\\)"));
        PATTERNS.put("CONVERT", Pattern.compile("(?i)CONVERT\\((\\w+)\\s*,\\s*([^)]+)\\)"));
        PATTERNS.put("IIF", Pattern.compile("(?i)IIF\\(([^,]+)\\s*,\\s*([^,]+)\\s*,\\s*([^)]+)\\)"));
        // 特殊语法
        PATTERNS.put("POINT", Pattern.compile("(?i)POINT\\s*\\(\\s*([\\d.]+)\\s+([\\d.]+)\\s*\\)"));
        PATTERNS.put("STRING_CONCAT", Pattern.compile("(?<=\\w|\\\")\\s*\\+\\s*(?=\\w|\\\")"));
        PATTERNS.put("NOT_EQUAL", Pattern.compile("!="));
        PATTERNS.put("TOP_CLAUSE", Pattern.compile("(?i)TOP\\s+\\(?(\\d+)\\)?\\s+"));
    }

    /**
     * convertDefinition
     *
     * @param postgresSqlDefinition postgresSqlDefinition
     * @return PostgreSQL Definition
     */
    public static String convertDefinition(String postgresSqlDefinition) {
        if (postgresSqlDefinition == null || postgresSqlDefinition.isEmpty()) {
            return postgresSqlDefinition;
        }
        String result = postgresSqlDefinition;
        // 1. 转换列名引用格式 [column] -> "column"
        result = PATTERNS.get("COLUMN_REF").matcher(result).replaceAll("$1");
        // 2. 转换日期时间函数
        result = PATTERNS.get("GETDATE").matcher(result).replaceAll("CURRENT_TIMESTAMP");
        result = PATTERNS.get("GETUTCDATE").matcher(result).replaceAll("CURRENT_TIMESTAMP");  // or CURRENT_TIMEZONE
        result = convertFunction(PATTERNS.get("DATEADD"), result, "$3 + INTERVAL '$2 $1'");
        result = convertFunction(PATTERNS.get("DATEDIFF"), result, "EXTRACT(EPOCH FROM ($3 - $2)) / 86400");
        result = convertFunction(PATTERNS.get("DATEPART"), result, "EXTRACT($1 FROM $2)");
        // 3. 转换字符串函数
        result = convertFunction(PATTERNS.get("LEN"), result, "LENGTH($1)");
        result = convertFunction(PATTERNS.get("CHARINDEX"), result, "POSITION($1 IN $2)");
        result = convertFunction(PATTERNS.get("SUBSTRING"), result, "SUBSTRING($1 FROM $2 FOR $3)");
        // 4. 转换其他函数
        result = convertFunction(PATTERNS.get("ISNULL"), result, "COALESCE($1, $2)");
        result = convertFunction(PATTERNS.get("CONVERT"), result, "CAST($2 AS $1)");
        result = convertFunction(PATTERNS.get("IIF"), result, "CASE WHEN $1 THEN $2 ELSE $3 END");
        // 5. 转换特殊语法
        result = convertFunction(PATTERNS.get("POINT"), result, "POINT($1, $2)");
        result = PATTERNS.get("STRING_CONCAT").matcher(result).replaceAll(" || ");
        result = PATTERNS.get("NOT_EQUAL").matcher(result).replaceAll("<>");
        result = PATTERNS.get("TOP_CLAUSE").matcher(result).replaceAll(""); // Remove TOP clause for PostgreSQL
        return result;
    }

    private static String convertFunction(Pattern pattern, String input, String replacement) {
        Matcher matcher = pattern.matcher(input);
        if (matcher.find()) {
            return matcher.replaceAll(replacement);
        }
        return input;
    }
}
