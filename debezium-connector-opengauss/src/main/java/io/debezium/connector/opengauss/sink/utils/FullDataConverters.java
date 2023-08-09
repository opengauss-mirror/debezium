/**
 * Copyright Debezium Authors.
 * <p>
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.opengauss.sink.utils;

import io.debezium.connector.opengauss.sink.object.ColumnMetaData;
import io.debezium.data.geometry.Point;
import io.debezium.util.HexConverter;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;

import java.nio.charset.Charset;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAccessor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static java.lang.Integer.toBinaryString;

/**
 * Description: Full data type converters
 *
 * @author czy
 * @date 2023/06/06
 **/
public class FullDataConverters {
    private static final String HEX_PREFIX = "x";
    private static final String HEX_FORMAT_PREFIX = "00000000";
    private static final long NANOSECOND_OF_DAY = 86400000000000L;
    private static final String INVALID_TIME_FORMAT_STRING = "HH:mm:ss.SSSSSSSSS";
    private static final String SINGLE_QUOTE = "'";
    private static final String BACKSLASH = "\\\\";

    private static Map<String, ObjectConverter> dataConverterMap = new HashMap<String, ObjectConverter>() {
        {
            put("integer", (columnName, value, after) -> objectConvertNumberType(columnName, value, after));
            put("tinyint", (columnName, value, after) -> objectConvertNumberType(columnName, value, after));
            put("double", (columnName, value, after) -> objectConvertNumberType(columnName, value, after));
            put("float", (columnName, value, after) -> objectConvertNumberType(columnName, value, after));
            put("tinyblob", (columnName, value, after) -> objectConvertBlob(columnName, value, after));
            put("mediumblob", (columnName, value, after) -> objectConvertBlob(columnName, value, after));
//            put("blob", (columnName, value, after) -> objectConvertBlob(columnName, value, after));
            put("longblob", (columnName, value, after) -> objectConvertBlob(columnName, value, after));
            put("datetime", (columnName, value, after) -> objectConvertDatetimeAndTimestamp(columnName, value, after));
            put("timestamp", (columnName, value, after) -> objectConvertDatetimeAndTimestamp(columnName, value, after));
            put("date", (columnName, value, after) -> objectConvertDate(columnName, value, after));
            put("time", (columnName, value, after) -> objectConvertTime(columnName, value, after));
            put("binary", (columnName, value, after) -> objectConvertBinary(columnName, value, after));
            put("varbinary", (columnName, value, after) -> objectConvertBinary(columnName, value, after));
            put("bit", (columnName, value, after) -> objectConvertBit(columnName, value, after));
//            put("set", (columnName, value, after) -> objectConvertSet(columnName, value, after));
            put("point", (columnName, value, after) -> objectConvertPoint(columnName, value, after));
            put("geometry", (columnName, value, after) -> objectConvertPoint(columnName, value, after));
            put("linestring", (columnName, value, after) -> objectConvertLinestring(columnName, value, after));
            put("polygon", (columnName, value, after) -> objectConvertPolygon(columnName, value, after));
            put("multipoint", (columnName, value, after) -> objectConvertMultipoint(columnName, value, after));
            put("multilinestring", (columnName, value, after) ->
                    objectConvertMultilinestring(columnName, value, after));
            put("multipolygon", (columnName, value, after) -> objectConvertMultipolygon(columnName, value, after));
            put("geometrycollection", (columnName, value, after) ->
                    objectConvertGeometrycollection(columnName, value, after));
        }
    };

    /**
     * Get value rewrite
     *
     * @param columnMetaData  the column metadata
     * @param value old value
     * @return new value
     */
    public static String getValue(ColumnMetaData columnMetaData, Object value, Struct after) {
        String columnName = columnMetaData.getColumnName();
        String columnType = columnMetaData.getColumnType();
        if (dataConverterMap.containsKey(columnType)) {
            return dataConverterMap.get(columnType).convert(columnName, value, after);
        }
        return value == null ? "" : addingSingleQuotation(value.toString());
    }

    private static String objectConvertNumberType(String columnName, Object value, Struct valueStruct) {
        if (value == null) {
            return "";
        }
        String object = String.valueOf(value);
        if (object.equalsIgnoreCase("true") || object.equalsIgnoreCase("false")) {
            return Boolean.parseBoolean(object) ? "1" : "0";
        }
        return addingSingleQuotation(object);
    }

    private static String objectConvertBlob(String columnName, Object value, Struct valueStruct) {
        if (value == null) {
            return "";
        }
        String str = String.valueOf(value);
        if (str.startsWith("\\x")) {
            str = str.substring(2);
        }
        return addingSingleQuotation(str);
    }

    private static String objectConvertDatetimeAndTimestamp(String columnName, Object value, Struct valueStruct) {
        Field field = valueStruct.schema().field(columnName);
        String schemaName = field.schema().name();
        if (value == null) {
            return "";
        }
        Instant instant = convertDbzDateTime(value, schemaName);
        DateTimeFormatter dateTimeFormatter;
        if ("io.debezium.time.ZonedTimestamp".equals(schemaName)) {
            dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS")
                    .withZone(ZoneId.of("Asia/Shanghai"));
        } else {
            dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS")
                    .withZone(ZoneOffset.UTC);
        }
        return addingSingleQuotation(dateTimeFormatter.format(instant));
    }

    private static String objectConvertDate(String columnName, Object value, Struct valueStruct) {
        Field field = valueStruct.schema().field(columnName);
        String schemaName = field.schema().name();
        if (value == null) {
            return "";
        }
        Instant instant = convertDbzDateTime(value, schemaName);
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
                .withZone(ZoneId.of("Asia/Shanghai"));
        return addingSingleQuotation(dateTimeFormatter.format(instant));
    }

    private static String objectConvertTime(String columnName, Object value, Struct valueStruct) {
        Field field = valueStruct.schema().field(columnName);
        String schemaName = field.schema().name();
        if (value == null){
            return "";
        }
        if ("io.debezium.time.MicroTime".equals(schemaName)) {
            long originMicro = Long.parseLong(value.toString()) * TimeUnit.MICROSECONDS.toNanos(1);
            if (originMicro >= NANOSECOND_OF_DAY) {
                return addingSingleQuotation(handleInvalidTime(originMicro));
            }
            if (originMicro < 0) {
                return addingSingleQuotation("-" + handleNegativeTime(-originMicro));
            }
        }
        Instant instant = convertDbzDateTime(value, schemaName);
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("HH:mm:ss").withZone(ZoneOffset.UTC);
        return addingSingleQuotation(dateTimeFormatter.format(instant));
    }

    private static String objectConvertBinary(String columnName, Object value, Struct valueStruct) {
        String str = String.valueOf(value);
        if (str.startsWith("\\x")) {
            str = str.substring(2);
        }
        return addingSingleQuotation(str);
    }

    private static String convertHexString(byte[] bytes) {
        return HexConverter.convertToHexString(bytes);
    }

    private static String objectConvertBit(String columnName, Object value, Struct after) {
        if (value == null) {
            return "";
        }
        String object = value.toString();
        if (object.equalsIgnoreCase("true") || object.equalsIgnoreCase("false")) {
            return Boolean.parseBoolean(value.toString()) ? "1" : "0";
        }
        return addingSingleQuotation(object);
    }

    private static String objectConvertSet(String columnName, Object value, Struct after) {
        return value == null ? "" : addingSingleQuotation(value);
    }

    private static String objectConvertPoint(String columnName, Object value, Struct after) {
        Object obj = value == null ? ""
                : formatPoint(Point.parseWKBPoint(value.toString().getBytes(Charset.defaultCharset())));
        return addingSingleQuotation(obj);
    }

    private static String objectConvertLinestring(String columnName, Object value, Struct after) {
        Field field = after.schema().field(columnName);
        String schemaName = field.schema().name();
        if (value == null) {
            return "";
        }
        byte[] bytes = value.toString().getBytes(Charset.defaultCharset());
        if (isGeometry(schemaName)) {
            return HEX_PREFIX + addingSingleQuotation(HEX_FORMAT_PREFIX + convertHexString(bytes));
        }
        String[] coordinateArr = getCoordinate(bytes);
        return addingSingleQuotation(formatLinestring(coordinateArr));
    }

    private static String objectConvertPolygon(String columnName, Object value, Struct after) {
        if (value == null) {
            return "";
        }
        Field field = after.schema().field(columnName);
        String schemaName = field.schema().name();
        byte[] bytes = value.toString().getBytes(Charset.defaultCharset());
        if (isGeometry(schemaName)) {
            return HEX_PREFIX + addingSingleQuotation(HEX_FORMAT_PREFIX + convertHexString(bytes));
        }
        String[] coordinateArr = getCoordinate(bytes);
        return addingSingleQuotation(formatPolygon(coordinateArr));
    }

    private static String objectConvertMultipoint(String columnName, Object value, Struct after) {
        Field field = after.schema().field(columnName);
        String schemaName = field.schema().name();
        byte[] bytes = value.toString().getBytes(Charset.defaultCharset());
        if (isGeometry(schemaName)) {
            return HEX_PREFIX + addingSingleQuotation(HEX_FORMAT_PREFIX + convertHexString(bytes));
        }
        String hexString = convertHexString(bytes);
        return HEX_PREFIX + addingSingleQuotation(new String(Objects.requireNonNull(parseHexStr2bytes(hexString))));
    }

    private static String objectConvertMultilinestring(String columnName, Object value, Struct after) {
        Field field = after.schema().field(columnName);
        String schemaName = field.schema().name();
        byte[] bytes = value.toString().getBytes(Charset.defaultCharset());
        if (isGeometry(schemaName)) {
            return HEX_PREFIX + addingSingleQuotation(HEX_FORMAT_PREFIX + convertHexString(bytes));
        }
        String hexString = convertHexString(bytes);
        return HEX_PREFIX + addingSingleQuotation(new String(Objects.requireNonNull(parseHexStr2bytes(hexString))));
    }

    private static String objectConvertMultipolygon(String columnName, Object value, Struct after) {
        Field field = after.schema().field(columnName);
        String schemaName = field.schema().name();
        byte[] bytes = value.toString().getBytes(Charset.defaultCharset());
        if (isGeometry(schemaName)) {
            return HEX_PREFIX + addingSingleQuotation(HEX_FORMAT_PREFIX + convertHexString(bytes));
        }
        String hexString = convertHexString(bytes);
        return HEX_PREFIX + addingSingleQuotation(new String(Objects.requireNonNull(parseHexStr2bytes(hexString))));
    }

    private static String objectConvertGeometrycollection(String columnName, Object value, Struct after) {
        Field field = after.schema().field(columnName);
        String schemaName = field.schema().name();
        byte[] bytes = value.toString().getBytes(Charset.defaultCharset());
        if (isGeometry(schemaName)) {
            return HEX_PREFIX + addingSingleQuotation(HEX_FORMAT_PREFIX + convertHexString(bytes));
        }
        String hexString = convertHexString(bytes);
        return HEX_PREFIX + addingSingleQuotation(new String(Objects.requireNonNull(parseHexStr2bytes(hexString))));
    }

    private static String[] getCoordinate(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        List<String> list = new ArrayList<>();
        for (byte aByte : bytes) {
            if (aByte == 46 || (aByte >= 48 && aByte <= 57)) {
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

    private static String formatPolygon(String[] coordinateArr) {
        return "ST_GeomFROMtEXT('POLYGON((" + formatCoordinate(coordinateArr) + "))')";
    }

    private static boolean isGeometry(String schemaName) {
        if ("io.debezium.data.geometry.Geometry".equals(schemaName)) {
            return true;
        }
        return false;
    }

    private static String formatLinestring(String[] coordinateArr) {
        return "ST_GeomFROMtEXT('LINESTRING(" + formatCoordinate(coordinateArr) + ")')";
    }

    private static String formatPoint(double[] coordinate) {
        return "ST_GeomFROMtEXT('POINT(" + coordinate[0] + " " + coordinate[1] + ")')";
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

    private static String convertBitString(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        sb.append(toBinaryString(adjustByte(bytes[bytes.length - 1])));
        if (bytes.length > 1) {
            for (int i = bytes.length - 2; i >= 0; i--) {
                sb.append(toBinaryString((bytes[i] & 0xFF) + 0x100).substring(1));
            }
        }
        return addingSingleQuotation(sb.toString());
    }

    private static int adjustByte(byte abyte) {
        return abyte >= 0 ? abyte : abyte + 256;
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

    private static Instant convertDbzDateTime(Object value, String schemaName) {
        Instant instant;
        LocalTime localTime;
        switch (schemaName) {
            case "io.debezium.time.Date":
                LocalDate localDate = LocalDate.ofEpochDay(Long.parseLong(value.toString()));
                instant = localDate.atStartOfDay().toInstant(ZoneOffset.UTC);
                break;
            case "io.debezium.time.MicroTimestamp":
                instant = Instant.EPOCH.plus(Long.parseLong(value.toString()), ChronoUnit.MICROS);
                break;
            case "io.debezium.time.ZonedTimestamp":
                String timeString = value.toString();
                if (timeString.contains("+")) {
                    DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ISO_OFFSET_DATE_TIME;
                    TemporalAccessor temporalAccessor = dateTimeFormatter.parse(timeString);
                    instant = Instant.from(temporalAccessor);
                } else {
                    instant = Instant.parse(timeString);
                }
                break;
            case "io.debezium.time.MicroTime":
                localTime = LocalTime.ofNanoOfDay(Long.parseLong(value.toString()) * TimeUnit.MICROSECONDS.toNanos(1));
                instant = localTime.atDate(LocalDate.now()).toInstant(ZoneOffset.UTC);
                break;
            case "io.debezium.time.ZonedTime":
                DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ISO_OFFSET_TIME;
                TemporalAccessor temporalAccessor = dateTimeFormatter.parse(value.toString());
                localTime = LocalTime.from(temporalAccessor);
                instant = localTime.atDate(LocalDate.now()).toInstant(ZoneOffset.UTC);
                break;
            case "io.debezium.time.Timestamp":
                instant = Instant.ofEpochMilli(Long.parseLong(value.toString()));
                break;
            case "io.debezium.time.Time":
                localTime = LocalTime.ofSecondOfDay(Long.parseLong(value.toString()) / 1000);
                instant = localTime.atDate(LocalDate.now()).toInstant(ZoneOffset.UTC);
                break;
            case "io.debezium.time.NanoTimestamp":
                instant = Instant.EPOCH.plus(Long.parseLong(value.toString()), ChronoUnit.NANOS);
                break;
            case "io.debezium.time.NanoTime":
                localTime = LocalTime.ofNanoOfDay(Long.parseLong(value.toString()));
                instant = localTime.atDate(LocalDate.now()).toInstant(ZoneOffset.UTC);
                break;
            default:
                return null;
        }
        return instant;
    }

    private static byte[] parseHexStr2bytes(String hexString) {
        if (hexString.length() < 1) {
            return new byte[0];
        }
        byte[] result = new byte[hexString.length() / 2];
        for (int i = 0; i < result.length; i++) {
            int high = Integer.parseInt(hexString.substring(2 * i, 2 * i + 1), 16);
            int low = Integer.parseInt(hexString.substring(2 * i + 1, 2 * i + 2), 16);
            result[i] = (byte) (high * 16 + low);
        }
        return result;
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

    private static byte[] string2ByteArray(String obj) {
        char[] chars = obj.toCharArray();
        byte[] bytes = new byte[chars.length];
        for (int i = 0; i < chars.length; i++) {
            bytes[i] = (byte) Integer.parseInt(chars[i] + "");
        }
        return bytes;
    }

    private static String addingSingleQuotation(Object originValue) {
        return SINGLE_QUOTE + originValue.toString()
                .replaceAll(SINGLE_QUOTE, SINGLE_QUOTE + SINGLE_QUOTE)
                .replaceAll(BACKSLASH, BACKSLASH + BACKSLASH) + SINGLE_QUOTE;
    }
}