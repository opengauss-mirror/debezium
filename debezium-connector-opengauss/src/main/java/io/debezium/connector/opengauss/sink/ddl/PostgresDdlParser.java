/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package io.debezium.connector.opengauss.sink.ddl;

import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * postgresql ddl parser
 *
 * @author tianbin
 * @since 2024-11-11
 */
public class PostgresDdlParser implements DdlParser {
    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresDdlParser.class);
    private final Pattern punctuation = Pattern.compile("\\p{P}");
    private final Map<String, String> schemaMappingMap;
    private String identifier;
    private String oldSchema;
    private String owner;
    private final String functionPrefix = "CREATE OR REPLACE FUNCTION ";
    private final String suffix = " RETURNS";
    private final String emptyWith = "  WITH ()";
    private final BigDecimal bigSerialMaxValue = BigDecimal.valueOf(9223372036854775807L);
    private final List<String> withWhiteList = Arrays.asList(
            "fillfactor",
            "autovacuum_vacuum_threshold",
            "autovacuum_analyze_threshold",
            "autovacuum_vacuum_cost_delay",
            "autovacuum_vacuum_cost_limit",
            "autovacuum_freeze_min_age",
            "autovacuum_freeze_max_age",
            "autovacuum_freeze_table_age",
            "autovacuum_vacuum_scale_factor",
            "autovacuum_analyze_scale_factor");
    private final String sequenceClausePrefix = "pg_catalog.nextval('";
    private boolean isTableRefreshed = false;
    private final Map<String, String> typeNameMappingMap = new HashMap<>();
    private final Map<String, JsonValueAdjuster> adjusterMap = new HashMap<String, JsonValueAdjuster>() {
        {
            put("function", value -> adjustFunction(value));
            put("objidentity", value -> adjustObjIdentity(value));
            put("identity", value -> adjustFieldComment(value));
            put("large", value -> adjustLargeSerial(value));
            put("default", value -> adjustTableClauseSequenceSchema(value));
            put("query", value -> adjustViewQuery(value));
        }
    };

    /**
     * Constructor
     *
     * @param schemaMappingMap schemaMap
     */
    public PostgresDdlParser(Map<String, String> schemaMappingMap) {
        this.schemaMappingMap = schemaMappingMap;
        initTypeNameMappingMap();
    }

    private void initTypeNameMappingMap() {
        typeNameMappingMap.put("int1", "int2");
        typeNameMappingMap.put("nvarchar2", "varchar");
        typeNameMappingMap.put("clob", "text");
        typeNameMappingMap.put("blob", "bytea");
        typeNameMappingMap.put("raw", "bytea");
        typeNameMappingMap.put("smalldatetime", "TIMESTAMP");
    }

    /**
     * Resolve JSON-formatted string
     *
     * @param jsonValue DDL in JSON format
     * @return Executable ddl statements
     */
    @Override
    public String parse(String jsonValue) {
        StringBuilder sb = new StringBuilder();
        if (!JSONObject.isValidObject(jsonValue)) {
            LOGGER.error("Invalid format of jsonValue '{}'", jsonValue);
            return sb.toString();
        }
        try {
            JSONObject json = JSONObject.parseObject(jsonValue);
            expandJsonRecursive(sb, json);
        } catch (JSONException | IndexOutOfBoundsException e) {
            LOGGER.error("ddl parse occurred error: ", e);
            return "";
        } catch (Exception e) {
            LOGGER.error("ddl parse occurred unknown error: ", e);
            return "";
        }
        String ddl = sb.append(";").toString().replace(emptyWith, "");
        LOGGER.info("The ddl currently being replayed is '{}'", ddl);
        return ddl;
    }

    private void expandJsonRecursive(StringBuilder result, JSONObject json) {
        String fmt = findStringInJsonObject(json, "fmt", false);
        if (isEmpty(fmt)) {
            result.append("''");
            return;
        }
        int end = fmt.length();
        for (int i = 0; i < end; i++) {
            if (fmt.charAt(i) != '%') {
                result.append(fmt.charAt(i));
                continue;
            }
            i++;
            if (fmt.charAt(i) == '%') {
                result.append(fmt.charAt(i));
                continue;
            }
            boolean isArray = false;
            String param = null;
            String arraySep = null;
            if (fmt.charAt(i) == '{') {
                StringBuilder paramBuilder = new StringBuilder();
                StringBuilder sepBuilder = new StringBuilder();
                StringBuilder appendTo = paramBuilder;
                i++;
                while (i < end) {
                    if (fmt.charAt(i) == ':') {
                        sepBuilder = new StringBuilder();
                        appendTo = sepBuilder;
                        isArray = true;
                        i++;
                        continue;
                    }
                    if (fmt.charAt(i) == '}') {
                        i++;
                        break;
                    }
                    appendTo.append(fmt.charAt(i));
                    i++;
                }
                param = paramBuilder.toString();
                if (isArray) {
                    arraySep = sepBuilder.toString();
                }
            }
            if (param == null) {
                LOGGER.error("Missing conversion name in conversion specifier");
                return;
            }
            ConversionSpecifier specifier = ConversionSpecifier.STRING;
            char c = fmt.charAt(i);
            switch (c) {
                case 'I':
                    specifier = ConversionSpecifier.IDENTIFIER;
                    break;
                case 'D':
                    specifier = ConversionSpecifier.DOTTED_NAME;
                    break;
                case 's':
                    specifier = ConversionSpecifier.STRING;
                    break;
                case 'L':
                    specifier = ConversionSpecifier.STRING_LITERAL;
                    break;
                case 'T':
                    specifier = ConversionSpecifier.TYPE_NAME;
                    break;
                case 'n':
                    specifier = ConversionSpecifier.NUMBER;
                    break;
                default:
                    LOGGER.warn("Invalid conversion specifier {}", c);
                    break;
            }
            String value = json.getString(param);
            if (isArray) {
                expandJsonArray(result, param, value, arraySep, specifier);
            } else {
                expandJsonElement(result, param, value, specifier);
            }
        }
    }

    private boolean expandJsonElement(StringBuilder result, String jsonKey, String jsonValue,
            ConversionSpecifier specifier) {
        if (jsonValue == null) {
            LOGGER.error("Element {} is not found", jsonKey);
        }
        boolean isStringExpanded = true;
        switch (specifier) {
            case IDENTIFIER:
                expandJsonToIdentifier(result, jsonValue);
                break;
            case DOTTED_NAME:
                expandJsonToDottedName(result, jsonKey, jsonValue);
                break;
            case STRING:
                isStringExpanded = expandJsonToString(result, jsonKey, jsonValue);
                break;
            case STRING_LITERAL:
                expandJsonToStringLiteral(result, jsonValue);
                break;
            case TYPE_NAME:
                expandJsonToTypeName(result, jsonValue);
                break;
            case NUMBER:
                expandJsonToNumber(result, jsonValue);
                break;
        }
        return isStringExpanded;
    }

    private void expandJsonToIdentifier(StringBuilder result, String jsonValue) {
        result.append(quoteIdentifier(jsonValue));
    }

    private String quoteIdentifier(String jsonValue) {
        StringBuilder sb = new StringBuilder("\"");
        for (char ch : jsonValue.toCharArray()) {
            if (ch == '"' || ch == '\\') {
                sb.append('\\');
            }
            sb.append(ch);
        }
        sb.append('"');
        return sb.toString();
    }

    private boolean isPunctuation(char ch) {
        return punctuation.matcher(String.valueOf(ch)).matches();
    }

    private void expandJsonToDottedName(StringBuilder result, String jsonKey, String jsonValue) {
        JSONObject json = JSONObject.parseObject(jsonValue);
        String schemaName = findStringInJsonObject(json, "schemaname", true);
        StringBuilder fullName = new StringBuilder();
        if (!isEmpty(schemaName)) {
            String newSchema = schemaMappingMap.getOrDefault(schemaName, schemaName);
            result.append(quoteIdentifier(newSchema)).append(".");
            fullName.append(newSchema).append(".");
        }
        String objName = findStringInJsonObject(json, "objname", false);
        if (!isEmpty(objName)) {
            result.append(quoteIdentifier(objName));
            fullName.append(objName);
        }
        if ("identity".equals(jsonKey)) {
            this.oldSchema = schemaName;
            identifier = fullName.toString();
        }
        if ("owner".equals(jsonKey)) {
            this.owner = fullName.toString();
        }
        String attrName = findStringInJsonObject(json, "attrname", true);
        if (!isEmpty(attrName)) {
            result.append(".").append(quoteIdentifier(attrName));
        }
    }

    private boolean expandJsonToString(StringBuilder result, String jsonKey, String jsonValue) {
        boolean isExpanded = false;
        if (JSONObject.isValidObject(jsonValue)) {
            JSONObject json = JSONObject.parseObject(jsonValue);
            String clause = json.getString("clause");
            if (clause != null && clause.equals("maxvalue")) {
                BigDecimal value = json.getBigDecimal("value");
                BigDecimal newValue = value.compareTo(bigSerialMaxValue) > 0 ? bigSerialMaxValue : value;
                json.put("value", newValue);
            }
            Boolean isPresent = json.getBoolean("present");
            if (!Boolean.FALSE.equals(isPresent)) {
                expandJsonRecursive(result, json);
                isExpanded = true;
            }
        } else {
            String value = jsonValue;
            if ("objtype".equals(jsonKey)) {
                isTableRefreshed = isTableRefreshed || jsonValue.equalsIgnoreCase("TABLE");
            }
            if (adjusterMap.containsKey(jsonKey)) {
                value = adjusterMap.get(jsonKey).adjust(jsonValue);
            }
            result.append(value);
            isExpanded = true;
        }
        return isExpanded;
    }

    private String adjustObjIdentity(String jsonValue) {
        String value = jsonValue;
        if (value.contains(" on ")) {
            // trigger
            String[] objArr = value.split(" on ");
            String fullName = objArr[1];
            String[] pair = fullName.split("\\.");
            String newSchema = schemaMappingMap.getOrDefault(pair[0], pair[0]);
            pair[0] = newSchema;
            objArr[1] = String.join(".", pair);
            value = String.join(" on ", objArr);
        } else {
            // other case
            String[] pair = value.split("\\.");
            this.oldSchema = pair[0];
            String newSchema = schemaMappingMap.getOrDefault(pair[0], pair[0]);
            pair[0] = newSchema;
            value = String.join(".", pair);
            this.identifier = value;
        }
        return value;
    }

    private String adjustFunction(String jsonValue) {
        int i = jsonValue.indexOf(functionPrefix) + functionPrefix.length();
        int j = jsonValue.indexOf(suffix);
        String functionName = jsonValue.substring(i, j).trim();
        String[] function = functionName.split("\\.");
        function[0] = schemaMappingMap.getOrDefault(function[0], function[0]);
        String newName = String.join(".", function);
        return jsonValue.replace(functionName, newName).replace("NOT FENCED NOT SHIPPABLE", "");
    }

    private String adjustFieldComment(String jsonValue) {
        // filed comment
        String[] comment = jsonValue.split("\\.");
        String newSchema = schemaMappingMap.getOrDefault(comment[0], comment[0]);
        comment[0] = newSchema;
        return String.join(".", comment);
    }

    private String adjustLargeSerial(String jsonValue) {
        // large serial
        if (jsonValue.equalsIgnoreCase("LARGE")) {
            LOGGER.warn("datatype 'LARGE SERIAL' is not supported in postgres, will use 'BIG SERIAL' instead.");
            return "";
        }
        return jsonValue;
    }

    private String adjustTableClauseSequenceSchema(String jsonValue) {
        // special handling for sequence clause in create table statement:
        // "default": "pg_catalog.nextval('public.t2_test_large_serial_seq'::pg_catalog.regclass)"
        if (jsonValue.startsWith(sequenceClausePrefix)) {
            String oldClausePrefix = sequenceClausePrefix + oldSchema;
            String newClausePrefix = sequenceClausePrefix + schemaMappingMap.getOrDefault(oldSchema, oldSchema);
            return jsonValue.replace(oldClausePrefix, newClausePrefix);
        }
        return jsonValue;
    }

    private String adjustViewQuery(String value) {
        for (String schema : schemaMappingMap.keySet()) {
            if (value.contains(schema + ".")) {
                // "query": "SELECT  * FROM public.t1 WHERE (t1.id OPERATOR(pg_catalog.<) 5);"
                return value.replace(schema + ".", schemaMappingMap.getOrDefault(schema, schema) + ".");
            }
        }
        return value;
    }

    private void expandJsonToStringLiteral(StringBuilder result, String jsonValue) {
        result.append("'").append(jsonValue).append("'");
    }

    private void expandJsonToTypeName(StringBuilder result, String jsonValue) {
        JSONObject json = JSONObject.parseObject(jsonValue);
        Boolean isArray = json.getBoolean("typarray");
        String decorator = "";
        if (isArray == null) {
            LOGGER.error("missing typearray element");
        }
        if (isArray) {
            decorator = "[]";
        }
        String schema = findStringInJsonObject(json, "schemaname", true);
        String typename = findStringInJsonObject(json, "typename", false);
        String newTypeName = typeNameMappingMap.getOrDefault(typename, typename);
        if (schema == null) {
            result.append(quoteIdentifier(newTypeName));
        } else if (schema.equals("")) {
            result.append(newTypeName);
        } else {
            result.append(schema).append(".").append(newTypeName);
        }
        String typmodstr = findStringInJsonObject(json, "typmod", true);
        result.append(typmodstr == null ? "" : typmodstr).append(decorator);
    }

    private void expandJsonToNumber(StringBuilder result, String jsonValue) {
        result.append(new BigDecimal(jsonValue));
    }

    private void expandJsonArray(StringBuilder result, String param, String value, String arraySep,
            ConversionSpecifier specifier) {
        List<String> jsonObjects = JSONObject.parseObject(value, new TypeReference<List<String>>() {
        });
        if ("with".equals(param)) {
            Iterator<String> iterator = jsonObjects.iterator();
            while (iterator.hasNext()) {
                String jsonString = iterator.next();
                if (!JSONObject.isValidObject(jsonString)) {
                    continue;
                }
                JSONObject labelObject = JSONObject.parseObject(jsonString).getJSONObject("label");
                if (labelObject == null) {
                    continue;
                }
                String label = labelObject.getString("label");
                if (!withWhiteList.contains(label)) {
                    LOGGER.warn("Unrecognized parameter '{}' will be ignored", label);
                    iterator.remove();
                }
            }
        }
        boolean isFirst = true;
        for (String jsonValue : jsonObjects) {
            StringBuilder element = new StringBuilder();
            if (expandJsonElement(element, param, jsonValue, specifier)) {
                if (!isFirst) {
                    result.append(arraySep);
                }
                result.append(element);
                isFirst = false;
            }
        }
    }

    private String findStringInJsonObject(JSONObject json, String key, boolean isMissOk) {
        String value = json.getString(key);
        if (value == null) {
            if (isMissOk) {
                return value;
            }
            LOGGER.error("missing element {} in JSON object", key);
        }
        return value;
    }

    private boolean isEmpty(String str) {
        return str == null || str.isEmpty();
    }

    /**
     * Get identifier
     *
     * @return objName
     */
    @Override
    public String identifier() {
        return identifier;
    }

    @Override
    public boolean isTableRefreshed() {
        return isTableRefreshed;
    }

    @Override
    public String owner() {
        return owner;
    }

    interface JsonValueAdjuster {
        /**
         * JsonValue adjuster
         *
         * @param jsonValue jsonValue
         * @return result
         */
        String adjust(String jsonValue);
    }
}
