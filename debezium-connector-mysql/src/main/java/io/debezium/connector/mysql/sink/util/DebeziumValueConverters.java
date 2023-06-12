/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.sink.util;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAccessor;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;

import io.debezium.connector.mysql.sink.object.ColumnMetaData;
import io.debezium.data.geometry.Geometry;
import io.debezium.data.geometry.Point;
import io.debezium.util.HexConverter;

import mil.nga.sf.wkb.GeometryReader;
import mil.nga.sf.wkt.GeometryWriter;

/**
 * Description: DebeziumValueConverters class
 *
 * @author douxin
 * @date 2023/01/14
 **/
public class DebeziumValueConverters {
    private static final char ESCAPE_CHARACTER = 'E';
    private static final String SINGLE_QUOTE = "'";
    private static final String HEX_PREFIX = "\\x";
    private static final String POLYGON_PREFIX = "POLYGON ";
    private static final String LINESTRING_PREFIX = "LINESTRING ";
    private static final String DATE_FORMAT_STRING = "yyyy-MM-dd";
    private static final String TIME_FORMAT_STRING = "HH:mm:ss";
    private static final String TIMESTAMP_FORMAT_STRING = "yyyy-MM-dd HH:mm:ss.SSSSSS";

    private static HashMap<String, ValueConverter> dataTypeConverterMap = new HashMap<String, ValueConverter>() {
        {
            put("uint1", (columnName, value) -> convertInteger(columnName, value));
            put("uint2", (columnName, value) -> convertInteger(columnName, value));
            put("uint4", (columnName, value) -> convertInteger(columnName, value));
            put("uint8", (columnName, value) -> convertInteger(columnName, value));
            put("tinyint", (columnName, value) -> convertInteger(columnName, value));
            put("smallint", (columnName, value) -> convertInteger(columnName, value));
            put("integer", (columnName, value) -> convertInteger(columnName, value));
            put("bigint", (columnName, value) -> convertInteger(columnName, value));
            put("character", (columnName, value) -> convertChar(columnName, value));
            put("tinyblob", (columnName, value) -> convertBinary(columnName, value));
            put("mediumblob", (columnName, value) -> convertBinary(columnName, value));
            put("blob", (columnName, value) -> convertBinary(columnName, value));
            put("longblob", (columnName, value) -> convertBinary(columnName, value));
            put("point", (columnName, value) -> convertPoint(columnName, value));
            put("path", (columnName, value) -> convertPath(columnName, value));
            put("polygon", (columnName, value) -> convertPolygon(columnName, value));
            put("\"binary\"", (columnName, value) -> convertBinary(columnName, value));
            put("\"varbinary\"", (columnName, value) -> convertBinary(columnName, value));
            put("binary", (columnName, value) -> convertBinary(columnName, value));
            put("varbinary", (columnName, value) -> convertBinary(columnName, value));
            put("bytea", (columnName, value) -> convertBytea(columnName, value));
            put("date", (columnName, value) -> convertDate(columnName, value));
            put("time without time zone", (columnName, value) -> convertTime(columnName, value));
            put("timestamp without time zone", (columnName, value) -> convertTimestamp(columnName, value));
            put("timestamp with time zone", (columnName, value) -> convertTimestamp(columnName, value));
            put("bit", ((columnName, value) -> convertBit(columnName, value)));
        }
    };

    /**
     * Get value
     *
     * @param ColumnMetaData the column metadata
     * @param Struct the struct value
     * @return String the value
     */
    public static String getValue(ColumnMetaData columnMetaData, Struct value) {
        String columnName = columnMetaData.getColumnName();
        String columnType = columnMetaData.getColumnType();
        if (dataTypeConverterMap.containsKey(columnType)) {
            return dataTypeConverterMap.get(columnType).convert(columnName, value);
        }
        if ("numeric".equals(columnType)) {
            Integer scale = columnMetaData.getScale();
            return convertNumeric(columnName, value, scale);
        }
        return convertChar(columnName, value);
    }

    private static String convertInteger(String columnName, Struct value) {
        Object object = value.get(columnName);
        return object == null ? null : object.toString();
    }

    private static String convertNumeric(String columnName, Struct value, Integer scale) {
        Object object = value.get(columnName);
        if (object == null) {
            return null;
        }
        String valueStr = object.toString();
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

    private static String convertChar(String columnName, Struct value) {
        Object object = value.get(columnName);
        return object == null ? null : addingSingleQuotation(object);
    }

    public static String addingSingleQuotation(Object originValue) {
        return SINGLE_QUOTE + originValue.toString().replaceAll(SINGLE_QUOTE, SINGLE_QUOTE + SINGLE_QUOTE)
                + SINGLE_QUOTE;
    }

    private static String convertBinary(String columnName, Struct value) {
        byte[] bytes = value.getBytes(columnName);
        return bytes == null ? null : convertHexString(bytes);
    }

    private static String convertHexString(byte[] bytes) {
        return addingSingleQuotation(HEX_PREFIX + HexConverter.convertToHexString(bytes));
    }

    private static String convertPoint(String columnName, Struct value) {
        // openGauss point -> mysql geometry,point
        // geometry: io.debezium.data.geometry.Geometry
        // point: io.debezium.data.geometry.Point
        Field field = value.schema().field(columnName);
        String schemaName = field.schema().name();
        Struct struct = value.getStruct(columnName);
        byte[] bytes;
        if ("io.debezium.data.geometry.Point".equals(schemaName)) {
            bytes = struct.getBytes(Point.WKB_FIELD);
        }
        else if ("io.debezium.data.geometry.Geometry".equals(schemaName)) {
            bytes = struct.getBytes(Geometry.WKB_FIELD);
        }
        else {
            return null;
        }
        return bytes == null ? null : formatPoint(Point.parseWKBPoint(bytes));
    }

    private static String formatPoint(double[] xyField) {
        return addingSingleQuotation("(" + xyField[0] + "," + xyField[1] + ")");
    }

    private static String convertPath(String columnName, Struct value) {
        // openGauss path -> mysql linestring
        Struct struct = value.getStruct(columnName);
        byte[] bytes = struct.getBytes(Geometry.WKB_FIELD);
        return bytes == null ? null : formatLinestring(wkb2Wkt(bytes));
    }

    private static String wkb2Wkt(byte[] bytes) {
        String wktResult = null;
        try {
            mil.nga.sf.Geometry geometry = GeometryReader.readGeometry(bytes);
            wktResult = GeometryWriter.writeGeometry(geometry);
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        return wktResult;
    }

    private static String formatLinestring(String originString) {
        if (originString == null) {
            return originString;
        }
        int linestring_prefix_len = LINESTRING_PREFIX.length();
        String modifiedString = "[" + originString.substring(linestring_prefix_len)
                .replaceAll(", ", "),(")
                .replaceAll(" ", ",")
                + "]";
        return addingSingleQuotation(modifiedString);
    }

    private static String convertPolygon(String columnName, Struct value) {
        // openGauss polygon -> mysql polygon
        Struct struct = value.getStruct(columnName);
        byte[] bytes = struct.getBytes(Geometry.WKB_FIELD);
        return bytes == null ? null : formatPolygon(wkb2Wkt(bytes));
    }

    private static String formatPolygon(String originString) {
        if (originString == null) {
            return null;
        }
        int polygon_prefix_len = POLYGON_PREFIX.length();
        String modifiedString = originString.substring(polygon_prefix_len)
                .replaceAll(", ", "),(")
                .replaceAll(" ", ",");
        return addingSingleQuotation(modifiedString);
    }

    private static String convertBytea(String columnName, Struct value) {
        // openGauss bytea -> mysql binary,varbinary,multipoint,geometrycollection,multilinestring,multipoint
        // binary, varbinary: value.getBytes
        // multipoint,geometrycollection,multilinestring,multipoint: value.getStruct.getBytes
        Field field = value.schema().field(columnName);
        String schemaName = field.schema().name();
        byte[] bytes;
        if (schemaName != null && schemaName.startsWith("io.debezium.data.geometry.")) {
            Struct struct = value.getStruct(columnName);
            bytes = struct.getBytes(Geometry.WKB_FIELD);
        }
        else {
            bytes = value.getBytes(columnName);
        }
        return bytes == null ? null : ESCAPE_CHARACTER + convertHexString(bytes);
    }

    private static String convertDate(String columnName, Struct value) {
        // openGauss date -> mysql date
        Instant instant = convertDbzDateTime(columnName, value);
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(DATE_FORMAT_STRING)
                .withZone(ZoneId.of("Asia/Shanghai"));
        return instant == null ? null : addingSingleQuotation(dateTimeFormatter.format(instant));
    }

    private static String convertTime(String columnName, Struct value) {
        // openGauss time without time zone -> mysql time
        Instant instant = convertDbzDateTime(columnName, value);
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(TIME_FORMAT_STRING)
                .withZone(ZoneOffset.UTC);
        return instant == null ? null : addingSingleQuotation(dateTimeFormatter.format(instant));
    }

    private static String convertTimestamp(String columnName, Struct value) {
        // openGauss timestamp without time zone -> mysql datetime, timestamp
        // datetime: io.debezium.time.timestamp
        // timestamp: io.debezium.time.ZonedTimestamp
        Instant instant = convertDbzDateTime(columnName, value);
        DateTimeFormatter dateTimeFormatter;
        Field field = value.schema().field(columnName);
        String schemaName = field.schema().name();
        if ("io.debezium.time.ZonedTimestamp".equals(schemaName)) {
            dateTimeFormatter = DateTimeFormatter.ofPattern(TIMESTAMP_FORMAT_STRING)
                    .withZone(ZoneId.of("Asia/Shanghai"));
        }
        else {
            dateTimeFormatter = DateTimeFormatter.ofPattern(TIMESTAMP_FORMAT_STRING)
                    .withZone(ZoneOffset.UTC);
        }
        return instant == null ? null : addingSingleQuotation(dateTimeFormatter.format(instant));
    }

    private static Instant convertDbzDateTime(String columnName, Struct value) {
        Field field = value.schema().field(columnName);
        String schemaName = field.schema().name();
        Object object = value.get(columnName);
        Instant instant;
        if (schemaName != null && object != null) {
            LocalTime localTime;
            switch (schemaName) {
                case "io.debezium.time.Date":
                    LocalDate localDate = LocalDate.ofEpochDay(Long.parseLong(object.toString()));
                    instant = localDate.atStartOfDay().toInstant(ZoneOffset.UTC);
                    break;
                case "io.debezium.time.MicroTimestamp":
                    instant = Instant.EPOCH.plus(Long.parseLong(object.toString()), ChronoUnit.MICROS);
                    break;
                case "io.debezium.time.ZonedTimestamp":
                    /*
                     * Two cases:
                     * A string representation of a timestamp with timezone information.
                     * A string representation of a timestamp in UTC.
                     */
                    String timeString = object.toString();
                    if (timeString.contains("+")) {
                        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ISO_OFFSET_DATE_TIME;
                        TemporalAccessor temporalAccessor = dateTimeFormatter.parse(timeString);
                        instant = Instant.from(temporalAccessor);
                    }
                    else {
                        instant = Instant.parse(timeString);
                    }
                    break;
                case "io.debezium.time.MicroTime":
                    localTime = LocalTime.ofNanoOfDay(Long.parseLong(object.toString()) * TimeUnit.MICROSECONDS.toNanos(1));
                    instant = localTime.atDate(LocalDate.now()).toInstant(ZoneOffset.UTC);
                    break;
                case "io.debezium.time.ZonedTime":
                    /*
                     * A string representation of a time value with timezone information, where the timezone is GMT.
                     */
                    DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ISO_OFFSET_TIME;
                    TemporalAccessor temporalAccessor = dateTimeFormatter.parse(object.toString());
                    localTime = LocalTime.from(temporalAccessor);
                    instant = localTime.atDate(LocalDate.now()).toInstant(ZoneOffset.UTC);
                    break;
                case "io.debezium.time.Timestamp":
                    instant = Instant.ofEpochMilli(Long.parseLong(object.toString()));
                    break;
                case "io.debezium.time.Time":
                    localTime = LocalTime.ofSecondOfDay(Long.parseLong(object.toString()) / 1000);
                    instant = localTime.atDate(LocalDate.now()).toInstant(ZoneOffset.UTC);
                    break;
                case "io.debezium.time.NanoTimestamp":
                    instant = Instant.EPOCH.plus(Long.parseLong(object.toString()), ChronoUnit.NANOS);
                    break;
                case "io.debezium.time.NanoTime":
                    localTime = LocalTime.ofNanoOfDay(Long.parseLong(object.toString()));
                    instant = localTime.atDate(LocalDate.now()).toInstant(ZoneOffset.UTC);
                    break;
                default:
                    return null;
            }
            return instant;
        }
        return null;
    }

    private static String convertBit(String columnName, Struct value) {
        byte[] bytes = value.getBytes(columnName);
        return bytes == null ? null : convertBitString(bytes);
    }

    private static String convertBitString(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte aByte : bytes) {
            sb.append(Integer.toBinaryString((aByte & 0xFF) + 0x100).substring(1));
        }
        return addingSingleQuotation(sb.toString());
    }
}
