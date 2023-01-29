/**
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.opengauss.sink.utils;

import io.debezium.data.geometry.Geometry;
import io.debezium.util.HexConverter;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;

import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAccessor;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Description: ValueConverter class
 * @author wangzhengyuan
 * @date 2023/01/17
 */
public final class ValueConverter {
    private ValueConverter(){}

    /**
     * Converts blob
     *
     * @param columnName String the column name
     * @param valueStruct Struct the value struct
     * @return String the column value
     */
    public static String convertBlob(String columnName, Struct valueStruct) {
        String hexString = convertBinaryToHex(columnName, valueStruct);
        if (hexString != null) {
            String originString = new String(Objects.requireNonNull(parseHexStr2bytes(hexString)));
            BigInteger value = new BigInteger(originString, 16);
            return addingSingleQuotation(value.toString());
        }
        return null;
    }

    /**
     * Converts binary
     *
     * @param columnName String the column name
     * @param value Struct the value
     * @return String the column value
     */
    public static String convertBinary(String columnName, Struct value){
        String hexString = convertBinaryToHex(columnName, value);
        return hexString == null ? null : addingSingleQuotation(new String(Objects.requireNonNull(parseHexStr2bytes(hexString))));
    }

    /**
     * Converts binary to hex string
     *
     * @param columnName String the column name
     * @param value Struct the value
     * @return String the hex string
     */
    public static String convertBinaryToHex(String columnName, Struct value) {
        Field field = value.schema().field(columnName);
        String schemaName = field.schema().name();
        byte[] bytes;
        if (schemaName != null && schemaName.startsWith("io.debezium.data.geometry.")){
            Struct struct = value.getStruct(columnName);
            bytes = struct.getBytes(Geometry.WKB_FIELD);
        } else {
            bytes = value.getBytes(columnName);
        }
        return bytes == null ? null : convertHexString(bytes);
    }

    private static String convertHexString(byte[] bytes) {
        return HexConverter.convertToHexString(bytes);
    }

    private static byte[] parseHexStr2bytes(String hexString) {
        if (hexString.length() < 1) {
            return null;
        }
        byte[] result = new byte[hexString.length() / 2];
        for (int i = 0; i < result.length; i++) {
            int high = Integer.parseInt(hexString.substring(2 * i, 2 * i + 1), 16);
            int low = Integer.parseInt(hexString.substring(2 * i + 1, 2 * i + 2), 16);
            result[i] = (byte)(high * 16 + low);
        }
        return result;
    }

    /**
     * Adds single quotation
     *
     * @param originValue String the origin value
     * @return String the value with single quotation
     */
    public static String addingSingleQuotation(Object originValue) {
        return "'" + originValue.toString() + "'";
    }

    /**
     * Converts datetime and timestamp
     *
     * @param columnName String the column name
     * @param valueStruct Struct the value struct
     * @return String the cinverted datetime or timestamp
     */
    public static String convertDatetimeAndTimestamp(String columnName, Struct valueStruct) {
        Field field = valueStruct.schema().field(columnName);
        String schemaName = field.schema().name();
        Object object = valueStruct.get(columnName);
        if (object == null){
            return null;
        }
        Instant instant = convertDbzDateTime(object, schemaName);
        DateTimeFormatter dateTimeFormatter;
        if ("io.debezium.time.ZonedTimestamp".equals(schemaName)){
            dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.of("Asia/Shanghai"));
        } else {
            dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneOffset.UTC);
        }
        return addingSingleQuotation(dateTimeFormatter.format(instant));
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
                }
                else {
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

    /**
     * Converts date
     *
     * @param columnName String the column name
     * @param valueStruct Struct the value struct
     * @return  String the converted date
     */
    public static String convertDate(String columnName, Struct valueStruct) {
        Field field = valueStruct.schema().field(columnName);
        String schemaName = field.schema().name();
        Object object = valueStruct.get(columnName);
        if (object == null){
            return null;
        }
        Instant instant = convertDbzDateTime(object, schemaName);
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(ZoneId.of("Asia/Shanghai"));
        return addingSingleQuotation(dateTimeFormatter.format(instant));
    }

    /**
     * Converts time
     *
     * @param columnName String the column name
     * @param valueStruct Struct the value struct
     * @return String the converted time
     */
    public static String convertTime(String columnName, Struct valueStruct) {
        Field field = valueStruct.schema().field(columnName);
        String schemaName = field.schema().name();
        Object object = valueStruct.get(columnName);
        if (object == null){
            return null;
        }
         Instant instant = convertDbzDateTime(object, schemaName);
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("HH:mm:ss").withZone(ZoneOffset.UTC);
        return addingSingleQuotation(dateTimeFormatter.format(instant));
    }

    /**
     * Converts tinyint
     *
     * @param columnName String the column name
     * @param valueStruct Struct the value struct
     * @return String the converted tinyint
     */
    public static String convertNumberType(String columnName, Struct valueStruct) {
        Object object = valueStruct.get(columnName);
        if (object != null){
            return object.toString();
        }
        return null;
    }
}
