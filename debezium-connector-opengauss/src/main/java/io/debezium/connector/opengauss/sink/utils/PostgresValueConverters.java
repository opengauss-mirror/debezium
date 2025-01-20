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

package io.debezium.connector.opengauss.sink.utils;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAccessor;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;

import io.debezium.connector.opengauss.sink.object.ColumnMetaData;
import io.debezium.data.geometry.Geometry;
import io.debezium.data.geometry.Point;
import io.debezium.util.HexConverter;
import org.apache.kafka.connect.errors.DataException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Description: PostgresValueConverter class
 *
 * @author tianbin
 * @since 2024/11/17
 */
public final class PostgresValueConverters {
    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresValueConverters.class);
    private static final char BIT_CHARACTER = 'b';
    private static final String HEX_PREFIX = "x";
    private static final long NANOSECOND_OF_DAY = 86400000000000L;
    private static final String INVALID_TIME_FORMAT_STRING = "HH:mm:ss.SSSSSSSSS";
    private static final String SINGLE_QUOTE = "'";
    private static final String BACKSLASH = "\\\\";
    private static final HashMap<String, ValueConverter> dataTypeConverterMap = new HashMap<String, ValueConverter>() {
        {
            put("integer", (columnName, value) -> convertNumberType(columnName, value));
            put("int", (columnName, value) -> convertIntType(columnName, value));
            put("tinyint", (columnName, value) -> convertIntType(columnName, value));
            put("smallint", (columnName, value) -> convertIntType(columnName, value));
            put("bigint", (columnName, value) -> convertIntType(columnName, value));
            put("double", (columnName, value) -> convertNumberType(columnName, value));
            put("float", (columnName, value) -> convertNumberType(columnName, value));
            put("blob", (columnName, value) -> convertBlob(columnName, value));
            put("datetime", (columnName, value) -> convertDatetimeAndTimestamp(columnName, value));
            put("timestamp", (columnName, value) -> convertDatetimeAndTimestamp(columnName, value));
            put("date", (columnName, value) -> convertDate(columnName, value));
            put("time", (columnName, value) -> convertTime(columnName, value));
            put("point", (columnName, value) -> convertPoint(columnName, value));
            put("time without time zone", (columnName, value) -> convertTime(columnName, value));
            put("real", ((columnName, value) -> convertNumberType(columnName, value)));
            put("timestamp without time zone", ((columnName, value) -> convertDatetimeAndTimestamp(columnName, value)));
            put("bytea", ((columnName, value) -> convertBytea(columnName, value)));
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
            put("USER-DEFINED", ((columnName, value) -> convertByte(columnName, value)));
        }
    };
    private static final String IO_DEBEZIUM_TIME_DATE = "io.debezium.time.Date";
    private static final String IO_DEBEZIUM_TIME_MICRO_TIMESTAMP = "io.debezium.time.MicroTimestamp";
    private static final String IO_DEBEZIUM_TIME_ZONED_TIMESTAMP = "io.debezium.time.ZonedTimestamp";
    private static final String IO_DEBEZIUM_TIME_MICRO_TIME = "io.debezium.time.MicroTime";
    private static final String IO_DEBEZIUM_TIME_ZONED_TIME = "io.debezium.time.ZonedTime";
    private static final String IO_DEBEZIUM_TIME_TIMESTAMP = "io.debezium.time.Timestamp";
    private static final String IO_DEBEZIUM_TIME_TIME = "io.debezium.time.Time";
    private static final String IO_DEBEZIUM_TIME_NANO_TIMESTAMP = "io.debezium.time.NanoTimestamp";
    private static final String IO_DEBEZIUM_TIME_NANO_TIME = "io.debezium.time.NanoTime";

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
            if ("numeric".equals(columnType)) {
                Integer scale = columnMetaData.getScale();
                return convertNumeric(columnName, value, scale);
            }
            if ("bit".equals(columnType) || "bit varying".equals(columnType)) {
                return convertBit(columnName, value, columnMetaData.getLength());
            }
        } catch (DataException | IndexOutOfBoundsException e) {
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

    private static String convertNumberType(String columnName, Struct valueStruct) {
        Object object = valueStruct.get(columnName);
        String result = null;
        if (object != null) {
            result = object.toString();
        }
        return result;
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
        String result = null;
        if (bytes != null) {
            String hexString = convertHexString(bytes);
            result = HEX_PREFIX + addingSingleQuotation(hexString);
        }
        return result;
    }

    private static String convertDatetimeAndTimestamp(String columnName, Struct valueStruct) {
        Field field = valueStruct.schema().field(columnName);
        String schemaName = field.schema().name();
        Object value = valueStruct.get(columnName);
        if (value == null) {
            return null;
        }
        Instant instant = convertDbzDateTime(value, schemaName);
        DateTimeFormatter dateTimeFormatter;
        if ("io.debezium.time.ZonedTimestamp".equals(schemaName)) {
            dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS")
                    .withZone(ZoneId.of("Asia/Shanghai"));
        } else {
            dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS").withZone(ZoneOffset.UTC);
        }
        return addingSingleQuotation(dateTimeFormatter.format(instant));
    }

    private static String convertDate(String columnName, Struct valueStruct) {
        Field field = valueStruct.schema().field(columnName);
        String schemaName = field.schema().name();
        Object object = valueStruct.get(columnName);
        if (object == null) {
            return null;
        }
        Instant instant = convertDbzDateTime(object, schemaName);
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
                .withZone(ZoneId.of("Asia/Shanghai"));
        return addingSingleQuotation(dateTimeFormatter.format(instant));
    }

    private static String convertTime(String columnName, Struct valueStruct) {
        Field field = valueStruct.schema().field(columnName);
        String schemaName = field.schema().name();
        Object object = valueStruct.get(columnName);
        if (object == null) {
            return null;
        }
        if ("io.debezium.time.MicroTime".equals(schemaName) || "io.debezium.time.Time".equals(schemaName)) {
            long originNano = getNanoOfTime(schemaName, object);
            if (originNano >= NANOSECOND_OF_DAY) {
                return addingSingleQuotation(handleInvalidTime(originNano));
            }
            if (originNano < 0) {
                return addingSingleQuotation("-" + handleNegativeTime(-originNano));
            }
        }
        Instant instant = convertDbzDateTime(object, schemaName);
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("HH:mm:ss").withZone(ZoneOffset.UTC);
        return addingSingleQuotation(dateTimeFormatter.format(instant));
    }

    private static long getNanoOfTime(String schemaName, Object object) {
        switch (schemaName) {
            case "io.debezium.time.MicroTime":
                return Long.parseLong(object.toString()) * TimeUnit.MICROSECONDS.toNanos(1);
            case "io.debezium.time.Time":
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
        Instant instant = null;
        LocalTime localTime;
        switch (schemaName) {
            case IO_DEBEZIUM_TIME_DATE:
                LocalDate localDate = LocalDate.ofEpochDay(Long.parseLong(value.toString()));
                instant = localDate.atStartOfDay().toInstant(ZoneOffset.UTC);
                break;
            case IO_DEBEZIUM_TIME_MICRO_TIMESTAMP:
                instant = Instant.EPOCH.plus(Long.parseLong(value.toString()), ChronoUnit.MICROS);
                break;
            case IO_DEBEZIUM_TIME_ZONED_TIMESTAMP:
                String timeString = value.toString();
                if (timeString.contains("+")) {
                    DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ISO_OFFSET_DATE_TIME;
                    TemporalAccessor temporalAccessor = dateTimeFormatter.parse(timeString);
                    instant = Instant.from(temporalAccessor);
                } else {
                    instant = Instant.parse(timeString);
                }
                break;
            case IO_DEBEZIUM_TIME_MICRO_TIME:
                localTime = LocalTime.ofNanoOfDay(Long.parseLong(value.toString()) * TimeUnit.MICROSECONDS.toNanos(1));
                instant = localTime.atDate(LocalDate.now()).toInstant(ZoneOffset.UTC);
                break;
            case IO_DEBEZIUM_TIME_ZONED_TIME:
                DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ISO_OFFSET_TIME;
                TemporalAccessor temporalAccessor = dateTimeFormatter.parse(value.toString());
                localTime = LocalTime.from(temporalAccessor);
                instant = localTime.atDate(LocalDate.now()).toInstant(ZoneOffset.UTC);
                break;
            case IO_DEBEZIUM_TIME_TIMESTAMP:
                instant = Instant.ofEpochMilli(Long.parseLong(value.toString()));
                break;
            case IO_DEBEZIUM_TIME_TIME:
                localTime = LocalTime.ofSecondOfDay(Long.parseLong(value.toString()) / 1000);
                instant = localTime.atDate(LocalDate.now()).toInstant(ZoneOffset.UTC);
                break;
            case IO_DEBEZIUM_TIME_NANO_TIMESTAMP:
                instant = Instant.EPOCH.plus(Long.parseLong(value.toString()), ChronoUnit.NANOS);
                break;
            case IO_DEBEZIUM_TIME_NANO_TIME:
                localTime = LocalTime.ofNanoOfDay(Long.parseLong(value.toString()));
                instant = localTime.atDate(LocalDate.now()).toInstant(ZoneOffset.UTC);
                break;
            default:
                return instant;
        }
        return instant;
    }

    private static String convertHexString(byte[] bytes) {
        return HexConverter.convertToHexString(bytes);
    }

    private static int adjustByte(byte abyte) {
        return abyte >= 0 ? abyte : abyte + 256;
    }

    private static String convertPoint(String columnName, Struct value) {
        Field field = value.schema().field(columnName);
        String schemaName = field.schema().name();
        Struct struct = value.getStruct(columnName);
        if (struct == null) {
            return null;
        }
        byte[] bytes;
        if ("io.debezium.data.geometry.Point".equals(schemaName)) {
            bytes = struct.getBytes(Point.WKB_FIELD);
        } else if (isGeometry(schemaName)) {
            bytes = struct.getBytes(Geometry.WKB_FIELD);
        } else {
            return null;
        }
        return bytes == null ? null : formatPoint(Point.parseWKBPoint(bytes));
    }

    private static String formatPoint(double[] coordinate) {
        return "'(" + coordinate[0] + "," + coordinate[1] + ")'";
    }

    private static boolean isGeometry(String schemaName) {
        return "io.debezium.data.geometry.Geometry".equals(schemaName);
    }

    private static String convertBytea(String columnName, Struct valueStruct) {
        byte[] bytes = valueStruct.getBytes(columnName);
        String result = null;
        if (bytes != null) {
            String hexString = convertHexString(bytes);
            result = SINGLE_QUOTE + "\\x" + hexString + SINGLE_QUOTE;
        }
        return result;
    }

    private static String convertByte(String columnName, Struct valueStruct) {
        byte[] bytes = valueStruct.getBytes(columnName);
        String result = null;
        if (bytes != null) {
            String byteStr = new String(bytes, StandardCharsets.UTF_8);
            result = addingSingleQuotation(byteStr);
        }
        return result;
    }

    private static String convertChar(String columnName, Struct value) {
        Object object = value.get(columnName);
        return object == null ? null : addingSingleQuotation(object);
    }

    private static String convertArray(String columnName, Struct value) {
        String arrayStr = convertChar(columnName, value);
        if (arrayStr == null) {
            return arrayStr;
        }
        return arrayStr.replace("[", "{").replace("]", "}");
    }

    private static String convertNumeric(String columnName, Struct value, Integer scale) {
        Object colValue = value.get(columnName);
        if (colValue == null) {
            return null;
        }
        String valueStr = colValue.toString();
        String decimalStr = valueStr.substring(valueStr.indexOf(".") + 1);
        if (scale == -1) {
            return colValue.toString();
        }
        if (decimalStr.length() > scale) {
            BigDecimal decimal = new BigDecimal(valueStr);
            BigDecimal result = decimal.setScale(scale, RoundingMode.HALF_UP);
            return result.toString();
        }
        return colValue.toString();
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
        if ("io.debezium.data.Bits".equals(schemaName)) {
            byte[] bytes = value.getBytes(columnName);
            return bytes == null ? null : convertBitString(bytes, length);
        }
        return null;
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

    private static String addingSingleQuotation(Object originValue) {
        return SINGLE_QUOTE + originValue.toString()
                .replaceAll(SINGLE_QUOTE, SINGLE_QUOTE + SINGLE_QUOTE)
                .replaceAll(BACKSLASH, BACKSLASH + BACKSLASH) + SINGLE_QUOTE;
    }
}
