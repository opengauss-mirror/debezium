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

package io.debezium.connector.postgresql.sink.utils;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.Year;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.TextStyle;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAccessor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import io.debezium.connector.postgresql.PostgresValueConverter;
import io.debezium.data.Bits;
import io.debezium.data.Json;
import io.debezium.time.Date;
import io.debezium.time.MicroTime;
import io.debezium.time.MicroTimestamp;
import io.debezium.time.NanoTime;
import io.debezium.time.NanoTimestamp;
import io.debezium.time.Time;
import io.debezium.time.Timestamp;
import io.debezium.time.ZonedTime;
import io.debezium.time.ZonedTimestamp;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;

import io.debezium.connector.postgresql.sink.object.ColumnMetaData;
import io.debezium.metadata.column.PostgresColumnType;
import io.debezium.data.geometry.Geometry;
import io.debezium.data.geometry.Point;
import io.debezium.util.HexConverter;
import org.apache.kafka.connect.errors.DataException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DebeziumValueConverters
 *
 * @author tianbin
 * @since 2024-11-25
 */
public final class DebeziumValueConverters {
    private static final Logger LOGGER = LoggerFactory.getLogger(DebeziumValueConverters.class);
    private static final char BIT_CHARACTER = 'b';
    private static final String HEX_PREFIX = "x";
    private static final String HEX_FORMAT_PREFIX = "00000000";
    private static final long NANOSECOND_OF_DAY = 86400000000000L;
    private static final String INVALID_TIME_FORMAT_STRING = "HH:mm:ss.SSSSSSSSS";
    private static final String SINGLE_QUOTE = "'";
    private static final String DOUBLE_QUOTE = "\"";
    private static final String DOUBLE_BACKSLASH = "\\\\";
    private static final String SINGLE_BACKSLASH = "\\";
    private static final byte DOT = 46;
    private static final byte ZERO = 48;
    private static final byte NINE = 57;
    private static final String JSONB_SUFFIX = "::jsonb";

    private DebeziumValueConverters() {
    }

    private static HashMap<String, ValueConverter> dataTypeConverterMap = new HashMap<String, ValueConverter>() {
        {
            put("integer", (columnName, value) -> convertNumberType(columnName, value));
            put("int", (columnName, value) -> convertIntType(columnName, value));
            put("tinyint", (columnName, value) -> convertIntType(columnName, value));
            put("smallint", (columnName, value) -> convertIntType(columnName, value));
            put("mediumint", (columnName, value) -> convertIntType(columnName, value));
            put("bigint", (columnName, value) -> convertIntType(columnName, value));
            put("double", (columnName, value) -> convertNumberType(columnName, value));
            put("float", (columnName, value) -> convertNumberType(columnName, value));
            put("blob", (columnName, value) -> convertBlob(columnName, value));
            put("datetime", (columnName, value) -> convertDatetimeAndTimestamp(columnName, value));
            put("timestamp", (columnName, value) -> convertDatetimeAndTimestamp(columnName, value));
            put("date", (columnName, value) -> convertDate(columnName, value));
            put("time", (columnName, value) -> convertTime(columnName, value));
            put("set", (columnName, value) -> convertSet(columnName, value));
            put("point", (columnName, value) -> convertPoint(columnName, value));
            put("geometry", (columnName, value) -> convertPoint(columnName, value));
            put("time without time zone", (columnName, value) -> convertTime(columnName, value));
            put("real", ((columnName, value) -> convertNumberType(columnName, value)));
            put("bytea", ((columnName, value) -> convertBytea(columnName, value)));
            put("timestamp without time zone", ((columnName, value) -> convertDatetimeAndTimestamp(columnName, value)));
            put("timestamp with time zone", ((columnName, value) -> convertTimestampTz(columnName, value)));
            put("lseg", ((columnName, value) -> convertByte(columnName, value)));
            put("box", ((columnName, value) -> convertByte(columnName, value)));
            put("path", ((columnName, value) -> convertByte(columnName, value)));
            put("polygon", (columnName, value) -> convertByte(columnName, value));
            put("circle", ((columnName, value) -> convertByte(columnName, value)));
            put("reltime", ((columnName, value) -> convertByte(columnName, value)));
            put("abstime", ((columnName, value) -> convertByte(columnName, value)));
            put("tsvector", ((columnName, value) -> convertByte(columnName, value)));
            put("tsquery", ((columnName, value) -> convertByte(columnName, value)));
            put("ARRAY", ((columnName, value) -> convertArray(columnName, value)));
            put("USER-DEFINED", ((columnName, value) -> convertUserDefined(columnName, value)));
            put("oid", ((columnName, value) -> convertOid(columnName, value)));
        }
    };

    /**
     * Get value
     *
     * @param columnMetaData ColumnMetaData the column metadata
     * @param value Struct the struct value
     * @return String the value
     */
    public static String getValue(ColumnMetaData columnMetaData, Struct value) {
        String columnName = columnMetaData.getColumnName();
        String columnType = columnMetaData.getColumnType();
        try {
            if (dataTypeConverterMap.containsKey(columnType)) {
                return dataTypeConverterMap.get(columnType).convert(columnName, value);
            }
            if (PostgresColumnType.PG_NUMERIC.code().equals(columnType)) {
                Integer scale = columnMetaData.getScale();
                return convertNumeric(columnName, value, scale);
            }
            if (PostgresColumnType.PG_BIT.code().equals(columnType)
                    || PostgresColumnType.PG_BIT_VARYING.code().equals(columnType)) {
                return convertBit(columnName, value, columnMetaData.getLength());
            }
        } catch (DataException e) {
            LOGGER.error("convert occurred exception, columnName: {}, columnType: {}, value: {}",
                    columnName, columnType, value.get(columnName), e);
            throw new DataException(e);
        } catch (Exception e) {
            LOGGER.error("convert occurred unknown exception, columnName: {}, columnType: {}, value: {}",
                    columnName, columnType, value.get(columnName), e);
            throw new DataException(e);
        }
        return convertChar(columnName, value);
    }

    private static String convertChar(String columnName, Struct value) {
        Object object = value.get(columnName);
        if (object == null) {
            return null;
        }
        if (object instanceof ByteBuffer) {
            return convertByte(columnName, value);
        }
        return addingSingleQuotation(object);
    }

    private static String convertArray(String columnName, Struct value) {
        Object object = value.get(columnName);
        if (object == null) {
            return null;
        }
        if (object instanceof String) {
            return addingSingleQuotation(object);
        }
        Field field = value.schema().field(columnName);
        String schemaName = field.schema().valueSchema().name();
        List<Object> jsonArray = value.getArray(columnName);
        if (Json.LOGICAL_NAME.equals(schemaName)) {
            StringBuilder sb = new StringBuilder("ARRAY[");
            for (int i = 0; i < jsonArray.size(); i++) {
                sb.append(addingSingleQuotation(jsonArray.get(i))).append(JSONB_SUFFIX);
                if (i != jsonArray.size() - 1) {
                    sb.append(",");
                }
            }
            return sb.append("]").toString();
        }
        StringBuilder sb = new StringBuilder("{");
        for (int i = 0; i < jsonArray.size(); i++) {
            sb.append(escapeSpecialChar(jsonArray.get(i).toString()));
            if (i != jsonArray.size() - 1) {
                sb.append(",");
            }
        }
        sb.append("}");
        return SINGLE_QUOTE + sb + SINGLE_QUOTE;
    }

    private static String convertNumberType(String columnName, Struct valueStruct) {
        Object object = valueStruct.get(columnName);
        if (object != null) {
            String valueStr = object.toString();
            if (!isNumeric(valueStr)) {
                // case of NaN, Infinity
                return addingSingleQuotation(valueStr);
            }
            return valueStr;
        }
        return null;
    }

    private static String convertIntType(String columnName, Struct valueStruct) {
        Field field = valueStruct.schema().field(columnName);
        String schemaName = field.schema().type().name();
        if ("bytes".equals(schemaName.toLowerCase(Locale.ROOT))) {
            byte[] bytes = valueStruct.getBytes(columnName);
            return bytes == null ? null : new String(bytes, StandardCharsets.UTF_8);
        }
        return convertNumberType(columnName, valueStruct);
    }

    private static String convertBlob(String columnName, Struct valueStruct) {
        byte[] bytes = valueStruct.getBytes(columnName);
        if (bytes != null) {
            String hexString = convertHexString(bytes);
            return HEX_PREFIX + addingSingleQuotation(hexString);
        }
        return null;
    }

    private static String convertTimestampTz(String columnName, Struct valueStruct) {
        Field field = valueStruct.schema().field(columnName);
        String schemaName = field.schema().name();
        Object value = valueStruct.get(columnName);
        if (value == null) {
            return null;
        }
        if (ZonedTimestamp.SCHEMA_NAME.equals(schemaName)) {
            String timeStr = valueStruct.getString(columnName);
            ZonedDateTime bcDateTime = ZonedDateTime.parse(timeStr);
            ZonedDateTime zonedDateTime = bcDateTime.withZoneSameInstant(ZoneId.of("Asia/Shanghai"));
            DateTimeFormatter bcFormatter = new DateTimeFormatterBuilder()
                    .appendValue(ChronoField.YEAR_OF_ERA)
                    .appendLiteral('-')
                    .appendPattern("MM-dd HH:mm:ss")
                    .appendLiteral(' ')
                    .appendText(ChronoField.ERA, TextStyle.SHORT)
                    .toFormatter(Locale.ENGLISH);
            return addingSingleQuotation(bcFormatter.format(zonedDateTime));
        }
        return addingSingleQuotation(value);
    }

    private static String convertDatetimeAndTimestamp(String columnName, Struct valueStruct) {
        Field field = valueStruct.schema().field(columnName);
        String schemaName = field.schema().name();
        Object object = valueStruct.get(columnName);
        if (object == null) {
            return null;
        }
        Instant instant = convertDbzDateTime(object, schemaName);
        if (instant == null) {
            return null;
        }
        if (PostgresValueConverter.POSITIVE_INFINITY_INSTANT.equals(instant)) {
            return addingSingleQuotation(PostgresValueConverter.POSITIVE_INFINITY);
        }
        if (PostgresValueConverter.NEGATIVE_INFINITY_INSTANT.equals(instant)) {
            return addingSingleQuotation(PostgresValueConverter.NEGATIVE_INFINITY);
        }
        if (isBeforeChrist(instant)) {
            return addingSingleQuotation(resolveTimestampBeforeChrist(instant, schemaName));
        }
        DateTimeFormatter dateTimeFormatter;
        if (ZonedTimestamp.SCHEMA_NAME.equals(schemaName)) {
            dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS")
                    .withZone(ZoneId.of("Asia/Shanghai"));
        } else {
            dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS").withZone(ZoneOffset.UTC);
        }
        return addingSingleQuotation(dateTimeFormatter.format(instant));
    }

    // -4712-01-01 00:00:00 -> 4713-01-01 00:00:00 BC
    private static String resolveTimestampBeforeChrist(Instant instant, String schemaName) {
        DateTimeFormatter bcFormatter = new DateTimeFormatterBuilder()
                .appendValue(ChronoField.YEAR_OF_ERA)
                .appendLiteral('-')
                .appendPattern("MM-dd HH:mm:ss.SSSSSS")
                .appendLiteral(' ')
                .appendText(ChronoField.ERA, TextStyle.SHORT)
                .toFormatter(Locale.ENGLISH);
        if (ZonedTimestamp.SCHEMA_NAME.equals(schemaName)) {
            bcFormatter = bcFormatter.withZone(ZoneId.of("Asia/Shanghai"));
        } else {
            bcFormatter = bcFormatter.withZone(ZoneOffset.UTC);
        }
        return bcFormatter.format(instant);
    }

    private static boolean isBeforeChrist(Instant instant) {
        Year year = Year.from(instant.atZone(ZoneOffset.UTC));
        // If the year is less than or equal to 0, it is BC (Before Christ).
        return year.getValue() <= 0;
    }

    private static String convertDate(String columnName, Struct valueStruct) {
        Field field = valueStruct.schema().field(columnName);
        String schemaName = field.schema().name();
        Object object = valueStruct.get(columnName);
        if (object == null) {
            return null;
        }
        Instant instant = convertDbzDateTime(object, schemaName);
        if (instant == null) {
            return null;
        }
        if (isBeforeChrist(instant)) {
            return addingSingleQuotation(resolveDateBeforeChrist(instant));
        }
        LocalDate localDate = instant.atZone(ZoneOffset.UTC).toLocalDate();
        if (PostgresValueConverter.POSITIVE_INFINITY_LOCAL_DATE.equals(localDate)) {
            return addingSingleQuotation(PostgresValueConverter.POSITIVE_INFINITY);
        }
        if (PostgresValueConverter.NEGATIVE_INFINITY_LOCAL_DATE.equals(localDate)) {
            return addingSingleQuotation(PostgresValueConverter.NEGATIVE_INFINITY);
        }
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
                .withZone(ZoneId.of("Asia/Shanghai"));
        String formatDate = dateTimeFormatter.format(instant);
        if (formatDate.startsWith("+")) {
            formatDate = formatDate.substring(1);
        }
        return addingSingleQuotation(formatDate);
    }

    // -4712-01-01 -> 4713-01-01 BC
    private static String resolveDateBeforeChrist(Instant instant) {
        DateTimeFormatter bcFormatter = new DateTimeFormatterBuilder()
                .appendValue(ChronoField.YEAR_OF_ERA)
                .appendLiteral('-')
                .appendPattern("MM-dd")
                .appendLiteral(' ')
                .appendText(ChronoField.ERA, TextStyle.SHORT)
                .toFormatter(Locale.ENGLISH);
        return bcFormatter.withZone(ZoneId.of("Asia/Shanghai")).format(instant);
    }

    private static String convertTime(String columnName, Struct valueStruct) {
        Field field = valueStruct.schema().field(columnName);
        String schemaName = field.schema().name();
        Object object = valueStruct.get(columnName);
        if (object == null) {
            return null;
        }
        if (MicroTime.SCHEMA_NAME.equals(schemaName) || Time.SCHEMA_NAME.equals(schemaName)) {
            long originNano = getNanoOfTime(schemaName, object);

            if (originNano >= NANOSECOND_OF_DAY) {
                return addingSingleQuotation(handleInvalidTime(originNano));
            }
            if (originNano < 0) {
                return addingSingleQuotation("-" + handleNegativeTime(-originNano));
            }
        }
        Instant instant = convertDbzDateTime(object, schemaName);
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("HH:mm:ss.SSSSSS").withZone(ZoneOffset.UTC);
        return addingSingleQuotation(dateTimeFormatter.format(instant));
    }

    private static long getNanoOfTime(String schemaName, Object object) {
        switch (schemaName) {
            case MicroTime.SCHEMA_NAME:
                return Long.parseLong(object.toString()) * TimeUnit.MICROSECONDS.toNanos(1);
            case Time.SCHEMA_NAME:
                return Long.parseLong(object.toString()) * 1000000;
            default:
                return 0;
        }
    }

    private static String handleInvalidTime(long originNano) {
        long validNano = originNano - NANOSECOND_OF_DAY;
        int days = 1;
        while (validNano >= NANOSECOND_OF_DAY) {
            validNano -= NANOSECOND_OF_DAY;
            days++;
        }
        LocalTime localTime = LocalTime.ofNanoOfDay(validNano);
        Instant instant = localTime.atDate(LocalDate.now()).toInstant(ZoneOffset.UTC);
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(INVALID_TIME_FORMAT_STRING)
                .withZone(ZoneOffset.UTC);
        String time = dateTimeFormatter.format(instant);
        return 24 * days + Integer.parseInt(time.split(":")[0])
                + time.substring(time.indexOf(":"));
    }

    private static String handleNegativeTime(long originNano) {
        if (originNano >= NANOSECOND_OF_DAY) {
            return handleInvalidTime(originNano);
        }
        LocalTime localTime = LocalTime.ofNanoOfDay(originNano);
        Instant instant = localTime.atDate(LocalDate.now()).toInstant(ZoneOffset.UTC);
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(INVALID_TIME_FORMAT_STRING)
                .withZone(ZoneOffset.UTC);
        return dateTimeFormatter.format(instant);
    }

    private static Instant convertDbzDateTime(Object value, String schemaName) {
        Instant instant;
        LocalTime localTime;
        switch (schemaName) {
            case Date.SCHEMA_NAME:
                LocalDate localDate = LocalDate.ofEpochDay(Long.parseLong(value.toString()));
                instant = localDate.atStartOfDay().toInstant(ZoneOffset.UTC);
                break;
            case MicroTimestamp.SCHEMA_NAME:
                instant = Instant.EPOCH.plus(Long.parseLong(value.toString()), ChronoUnit.MICROS);
                break;
            case ZonedTimestamp.SCHEMA_NAME:
                String timeString = value.toString();
                if (timeString.contains("+")) {
                    DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ISO_OFFSET_DATE_TIME;
                    TemporalAccessor temporalAccessor = dateTimeFormatter.parse(timeString);
                    instant = Instant.from(temporalAccessor);
                }
                else {
                    instant = Instant.parse(timeString);
                }
                break;
            case MicroTime.SCHEMA_NAME:
                localTime = LocalTime.ofNanoOfDay(Long.parseLong(value.toString()) * TimeUnit.MICROSECONDS.toNanos(1));
                instant = localTime.atDate(LocalDate.now()).toInstant(ZoneOffset.UTC);
                break;
            case ZonedTime.SCHEMA_NAME:
                DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ISO_OFFSET_TIME;
                TemporalAccessor temporalAccessor = dateTimeFormatter.parse(value.toString());
                localTime = LocalTime.from(temporalAccessor);
                instant = localTime.atDate(LocalDate.now()).toInstant(ZoneOffset.UTC);
                break;
            case Timestamp.SCHEMA_NAME:
                instant = Instant.ofEpochMilli(Long.parseLong(value.toString()));
                break;
            case Time.SCHEMA_NAME:
                localTime = LocalTime.ofSecondOfDay(Long.parseLong(value.toString()) / 1000);
                instant = localTime.atDate(LocalDate.now()).toInstant(ZoneOffset.UTC);
                break;
            case NanoTimestamp.SCHEMA_NAME:
                instant = Instant.EPOCH.plus(Long.parseLong(value.toString()), ChronoUnit.NANOS);
                break;
            case NanoTime.SCHEMA_NAME:
                localTime = LocalTime.ofNanoOfDay(Long.parseLong(value.toString()));
                instant = localTime.atDate(LocalDate.now()).toInstant(ZoneOffset.UTC);
                break;
            default:
                return null;
        }
        return instant;
    }

    private static String convertByte(String columnName, Struct valueStruct) {
        byte[] bytes = valueStruct.getBytes(columnName);
        if (bytes != null) {
            String byteStr = new String(bytes, StandardCharsets.UTF_8);
            return addingSingleQuotation(byteStr);
        }
        return null;
    }

    private static String convertOid(String columnName, Struct valueStruct) {
        Object object = valueStruct.get(columnName);
        if (object == null) {
            return null;
        }
        if (object instanceof Number) {
            return String.valueOf(object);
        }
        if (object instanceof byte[]) {
            byte[] bytes = (byte[]) object;
            String byteStr = new String(bytes, StandardCharsets.UTF_8);
            return addingSingleQuotation(byteStr);
        }
        return object.toString();
    }

    private static String convertUserDefined(String columnName, Struct valueStruct) {
        Object object = valueStruct.get(columnName);
        if (object == null) {
            return null;
        }
        if (object instanceof ByteBuffer) {
            byte[] bytes = valueStruct.getBytes(columnName);
            return addingSingleQuotation(new String(bytes, StandardCharsets.UTF_8));
        }
        if (object instanceof byte[]) {
            byte[] bytes = (byte[]) object;
            String byteStr = new String(bytes, StandardCharsets.UTF_8);
            return addingSingleQuotation(byteStr);
        }
        if (object instanceof String) {
            return addingSingleQuotation(object);
        }
        return object.toString();
    }

    private static String convertBytea(String columnName, Struct valueStruct) {
        byte[] bytes = valueStruct.getBytes(columnName);
        if (bytes != null) {
            String hexStr = convertHexString(bytes);
            return SINGLE_QUOTE + "\\x" + hexStr + SINGLE_QUOTE;
        }
        return null;
    }

    private static String convertHexString(byte[] bytes) {
        return HexConverter.convertToHexString(bytes);
    }

    private static String convertBit(String columnName, Struct value, int length) {
        if (length == 1) {
            Boolean isTrue = value.getBoolean(columnName);
            if (isTrue == null) {
                return null;
            }
            byte bit = (byte) (isTrue ? 1 : 0);
            return convertBitString(new byte[]{bit}, length);
        }
        Field field = value.schema().field(columnName);
        String schemaName = field.schema().name();
        byte[] bytes;
        if (Bits.LOGICAL_NAME.equals(schemaName)) {
            bytes = value.getBytes(columnName);
            return bytes == null ? null : convertBitString(bytes, length);
        }
        return null;
    }

    private static int adjustByte(byte abyte) {
        return abyte >= 0 ? abyte : abyte + 256;
    }

    private static String convertBitString(byte[] bytes, int length) {
        StringBuilder sb = new StringBuilder();
        if (bytes.length == 0) {
            for (int i = 0; i < length; i++) {
                sb.append("0");
            }
            return BIT_CHARACTER + addingSingleQuotation(sb.toString());
        }
        sb.append(Integer.toBinaryString(adjustByte(bytes[bytes.length - 1])));
        if (bytes.length > 1) {
            for (int i = bytes.length - 2; i >= 0; i--) {
                sb.append(Integer.toBinaryString((bytes[i] & 0xFF) + 0x100).substring(1));
            }
        }
        if (length > sb.length()) {
            char[] padded = new char[length - sb.length()];
            Arrays.fill(padded, '0');
            sb.insert(0, padded);
        }
        return BIT_CHARACTER + addingSingleQuotation(sb.toString());
    }

    private static String convertSet(String columnName, Struct valueStruct) {
        byte[] bytes = valueStruct.getBytes(columnName);
        return bytes == null ? null : addingSingleQuotation(new String(bytes, StandardCharsets.UTF_8));
    }

    private static String convertPoint(String columnName, Struct value) {
        Field field = value.schema().field(columnName);
        String schemaName = field.schema().name();
        Struct struct = value.getStruct(columnName);
        if (struct == null) {
            return null;
        }
        byte[] bytes;
        if (Point.LOGICAL_NAME.equals(schemaName)) {
            bytes = struct.getBytes(Point.WKB_FIELD);
        } else if (isGeometry(schemaName)) {
            bytes = struct.getBytes(Geometry.WKB_FIELD);
        } else {
            return null;
        }
        return bytes == null ? null : formatPoint(Point.parseWKBPoint(bytes));
    }

    private static String formatPoint(double[] coordinate) {
        String format = "'(%s,%s)'";
        return String.format(format, coordinate[0], coordinate[1]);
    }

    private static String convertLinestring(String columnName, Struct valueStruct) {
        Field field = valueStruct.schema().field(columnName);
        String schemaName = field.schema().name();
        if (isGeometry(schemaName)) {
            return HEX_PREFIX + convertGeometry(columnName, valueStruct);
        }
        byte[] bytes = valueStruct.getBytes(columnName);
        if (bytes == null) {
            return null;
        }
        String[] coordinateArr = getCoordinate(bytes);
        return formatLinestring(coordinateArr);
    }

    private static String formatLinestring(String[] coordinateArr) {
        String format = "ST_GeomFROMtEXT('LINESTRING(%s)')";
        return String.format(format, formatCoordinate(coordinateArr));
    }

    private static String convertGeometry(String columnName, Struct valueStruct) {
        Struct struct = valueStruct.getStruct(columnName);
        byte[] bytes = struct.getBytes(Geometry.WKB_FIELD);
        if (bytes == null) {
            return null;
        }
        return addingSingleQuotation(HEX_FORMAT_PREFIX + convertHexString(bytes));
    }

    private static String[] getCoordinate(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        List<String> list = new ArrayList<>();
        for (byte aByte : bytes) {
            if (aByte == DOT || (aByte >= ZERO && aByte <= NINE)) {
                sb.append((char) aByte);
            } else if (sb.length() > 0) {
                list.add(sb.toString());
                sb.setLength(0);
            } else {
                sb.setLength(0);
            }
        }
        return list.toArray(new String[0]);
    }

    private static String formatCoordinate(String[] coordinateArr) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < coordinateArr.length - 1; i += 2) {
            sb.append(coordinateArr[i]);
            sb.append(" ");
            sb.append(coordinateArr[i + 1]);
            sb.append(",");
        }
        sb = new StringBuilder(sb.substring(0, sb.lastIndexOf(",")));
        return sb.toString();
    }

    private static boolean isGeometry(String schemaName) {
        if (Geometry.LOGICAL_NAME.equals(schemaName)) {
            return true;
        }
        return false;
    }

    private static String convertNumeric(String columnName, Struct value, Integer scale) {
        Object object = value.get(columnName);
        if (object == null) {
            return null;
        }
        String valueStr = object.toString();
        if (!isNumeric(valueStr)) {
            // case of NaN, Infinity
            return addingSingleQuotation(valueStr);
        }
        String decimalStr = valueStr.substring(valueStr.indexOf(".") + 1);
        if (scale == -1) {
            return object.toString();
        }
        if (decimalStr.length() > scale) {
            BigDecimal decimal = new BigDecimal(valueStr);
            BigDecimal result = decimal.setScale(scale, RoundingMode.HALF_UP);
            return result.toString();
        }
        return object.toString();
    }

    private static boolean isNumeric(final String str) {
        if (str.isEmpty()) {
            return false;
        }
        final int len = str.length();
        for (int i = 0; i < len; i++) {
            if (!Character.isDigit(str.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    private static String escapeSpecialChar(String data) {
        String escapeStr = data.replace(SINGLE_QUOTE, SINGLE_QUOTE + SINGLE_QUOTE)
                .replace(SINGLE_BACKSLASH, DOUBLE_BACKSLASH)
                .replace(DOUBLE_QUOTE, SINGLE_BACKSLASH + DOUBLE_QUOTE);
        return DOUBLE_QUOTE + escapeStr + DOUBLE_QUOTE;
    }

    private static String addingSingleQuotation(Object originValue) {
        return SINGLE_QUOTE + originValue.toString()
                .replaceAll(SINGLE_QUOTE, SINGLE_QUOTE + SINGLE_QUOTE)
                .replaceAll(DOUBLE_BACKSLASH, DOUBLE_BACKSLASH + DOUBLE_BACKSLASH) + SINGLE_QUOTE;
    }
}
