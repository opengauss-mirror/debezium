/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.sink.util;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalTime;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAccessor;
import java.util.HashMap;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import io.debezium.time.Date;
import io.debezium.time.MicroDuration;
import io.debezium.time.MicroTimestamp;
import io.debezium.time.ZonedTimestamp;
import io.debezium.time.ZonedTime;
import io.debezium.time.Timestamp;
import io.debezium.time.Time;
import io.debezium.time.NanoTime;
import io.debezium.time.NanoTimestamp;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;

import io.debezium.connector.oracle.sink.object.ColumnMetaData;
import io.debezium.data.SpecialValueDecimal;
import io.debezium.data.VariableScaleDecimal;
import io.debezium.data.geometry.Geometry;
import io.debezium.util.HexConverter;

/**
 * Description: DebeziumValueConverters class
 *
 * @author gbase
 * @date 2023/07/31
 **/
public class DebeziumValueConverters {
    private static final char ESCAPE_CHARACTER = 'E';
    private static final String SINGLE_QUOTE = "'";
    private static final String HEX_PREFIX = "\\x";
    private static final String DATE_FORMAT_STRING = "yyyy-MM-dd";
    private static final String TIME_FORMAT_STRING = "HH:mm:ss.SSSSSSSSS";
    private static final String TIMESTAMP_FORMAT_STRING = "yyyy-MM-dd HH:mm:ss.SSSSSS";
    private static final long NANOSECOND_OF_DAY = 86400000000000L;
    private static final long EQUATION_OF_TIME = 48576000L;
    private static final String INVALID_TIME_FORMAT_STRING = "HH:mm:ss.SSSSSS";

    private static final HashMap<String, ValueConverter> dataTypeConverterMap = new HashMap<String, ValueConverter>() {
        {
            // put("uint1", DebeziumValueConverters::convertInteger);
            // put("uint2", DebeziumValueConverters::convertInteger);
            // put("uint4", DebeziumValueConverters::convertInteger);
            // put("uint8", DebeziumValueConverters::convertInteger);
            // put("tinyint", DebeziumValueConverters::convertInteger);
            // put("smallint", DebeziumValueConverters::convertInteger);
            // put("integer", DebeziumValueConverters::convertInteger);
            // put("bigint", DebeziumValueConverters::convertInteger);
            put("character", DebeziumValueConverters::convertChar);
            put("tinyblob", DebeziumValueConverters::convertBlob);
            put("mediumblob", DebeziumValueConverters::convertBlob);
            put("blob", DebeziumValueConverters::convertBlob);
            put("longblob", DebeziumValueConverters::convertBlob);
            put("\"binary\"", DebeziumValueConverters::convertBinary);
            put("\"varbinary\"", DebeziumValueConverters::convertBinary);
            put("binary", DebeziumValueConverters::convertBinary);
            put("varbinary", DebeziumValueConverters::convertBinary);
            put("bytea", DebeziumValueConverters::convertBytea);
            put("timestamp without time zone", DebeziumValueConverters::convertTimestamp);
            put("timestamp with time zone", DebeziumValueConverters::convertTimestamp);
            put("bit", (DebeziumValueConverters::convertBit));
        }
    };

    /**
     * Get value
     *
     * @param columnMetaData the column metadata
     * @param value the struct value
     * @return String the value
     */
    public static String getValue(ColumnMetaData columnMetaData, Struct value) {
        String columnName = columnMetaData.getColumnName();
        String columnType = columnMetaData.getColumnType();
        if (dataTypeConverterMap.containsKey(columnType)) {
            return dataTypeConverterMap.get(columnType).convert(columnName, value);
        }

        if ("numeric".equals(columnType) || "double precision".equals(columnType)
                || "integer".equals(columnType) || "smallint".equals(columnType)
                || "real".equals(columnType)) {
            Integer scale = columnMetaData.getScale();
            return convertNumeric(columnName, value, scale);
        }

        if ("interval".equals(columnType)) {
            String intervalType = columnMetaData.getIntervalType();
            return convertInterval(columnName, value, intervalType);
        }

        return convertChar(columnName, value);
    }

    private static String convertInteger(String columnName, Struct value) {
        Object object = value.get(columnName);
        return object == null ? null : object.toString();
    }

    private static String convertInterval(String columnName, Struct value, String intervalType) {
        Duration duration = Duration.of(value.getInt64(columnName), ChronoUnit.MICROS);

        switch (intervalType) {
            case "DAY TO SECOND":
                return addingSingleQuotation(duration.getSeconds());
            case "YEAR TO MONTH":
                double result = duration.getSeconds() / 60d / 60d / 24d / MicroDuration.DAYS_PER_MONTH_AVG;
                return addingSingleQuotation(result);
            default:
                return null;
        }
    }

    private static String convertNumeric(String columnName, Struct value, Integer scale) {
        Object object = value.get(columnName);
        if (object == null) {
            return null;
        }

        Field field = value.schema().field(columnName);
        String schemaName = field.schema().name();
        if (VariableScaleDecimal.LOGICAL_NAME.equals(schemaName)) {
            SpecialValueDecimal specialValueDecimal = VariableScaleDecimal.toLogical(value.getStruct(columnName));
            Optional<BigDecimal> decimalValue = specialValueDecimal.getDecimalValue();
            return decimalValue.map(BigDecimal::toString).orElse(null);
        }
        else if (Decimal.LOGICAL_NAME.equals(schemaName)) {
            return object.toString();
        }
        else {
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
    }

    private static String convertChar(String columnName, Struct value) {
        Object object = value.get(columnName);
        return object == null ? null : addingSingleQuotation(object);
    }

    /**
     * Adding single quotation
     *
     * @param originValue the origin value
     * @return String
     */
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

    private static String convertBlob(String columnName, Struct valueStruct) {
        try {
            byte[] bytes = valueStruct.getBytes(columnName);
            if (bytes != null) {
                String hexStr = HexConverter.convertToHexString(bytes);
                return "hextoraw(" + addingSingleQuotation(hexStr) + ")::blob";
            }
        } catch (DataException e) {
            // for longraw
            return valueStruct.getString(columnName);
        }

        return null;
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
        Field field = value.schema().field(columnName);
        String schemaName = field.schema().name();
        Instant instant;
        if ("io.debezium.time.MicroTime".equals(schemaName)) {
            Object object = value.get(columnName);
            if (object == null) {
                return null;
            }
            long originMicro = Long.parseLong(object.toString()) * TimeUnit.MICROSECONDS.toNanos(1);
            if (originMicro >= NANOSECOND_OF_DAY) {
                return addingSingleQuotation(handleInvalidTime(originMicro));
            }
            else if (originMicro < 0) {
                originMicro = (-originMicro - EQUATION_OF_TIME) % 1000000000 == 0
                        ? -originMicro - EQUATION_OF_TIME
                        : -originMicro;
                return addingSingleQuotation("-" + handleNegativeTime(originMicro));
            }
            else {
                LocalTime localTime = LocalTime.ofNanoOfDay(originMicro);
                instant = localTime.atDate(LocalDate.now()).toInstant(ZoneOffset.UTC);
            }
        }
        else {
            instant = convertDbzDateTime(columnName, value);
        }
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(TIME_FORMAT_STRING)
                .withZone(ZoneOffset.UTC);
        return instant == null ? null : addingSingleQuotation(dateTimeFormatter.format(instant));
    }

    private static String handleNegativeTime(long originNano) {
        if (originNano >= NANOSECOND_OF_DAY) {
            return handleInvalidTime(originNano);
        }
        LocalTime localTime = LocalTime.ofNanoOfDay(originNano);
        Instant instant = localTime.atDate(LocalDate.now()).toInstant(ZoneOffset.UTC);
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(TIME_FORMAT_STRING)
                .withZone(ZoneOffset.UTC);
        return dateTimeFormatter.format(instant);
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
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(TIME_FORMAT_STRING)
                .withZone(ZoneOffset.UTC);
        String time = dateTimeFormatter.format(instant);
        return 24 * days + Integer.parseInt(time.split(":")[0])
                + time.substring(time.indexOf(":"));
    }

    private static String convertTimestamp(String columnName, Struct value) {
        // openGauss timestamp without time zone -> oracle date, timestamp
        // date: io.debezium.time.Timestamp
        // timestamp: io.debezium.time.ZonedTimestamp
        DateTimeFormatter dateTimeFormatter;
        Field field = value.schema().field(columnName);
        String schemaName = field.schema().name();
        if (ZonedTimestamp.SCHEMA_NAME.equals(schemaName)) {
            dateTimeFormatter = DateTimeFormatter.ofPattern(TIMESTAMP_FORMAT_STRING)
                    .withZone(ZoneId.of("Asia/Shanghai"));
        }
        else {
            dateTimeFormatter = DateTimeFormatter.ofPattern(TIMESTAMP_FORMAT_STRING)
                    .withZone(ZoneOffset.UTC);
        }
        Instant instant = convertDbzDateTime(columnName, value);
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
                case Date.SCHEMA_NAME:
                    LocalDate localDate = LocalDate.ofEpochDay(Long.parseLong(object.toString()));
                    instant = localDate.atStartOfDay().toInstant(ZoneOffset.UTC);
                    break;
                case MicroTimestamp.SCHEMA_NAME:
                    instant = Instant.EPOCH.plus(Long.parseLong(object.toString()), ChronoUnit.MICROS);
                    break;
                case ZonedTimestamp.SCHEMA_NAME:
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
                case ZonedTime.SCHEMA_NAME:
                    /*
                     * A string representation of a time value with timezone information, where the timezone is GMT.
                     */
                    DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ISO_OFFSET_TIME;
                    TemporalAccessor temporalAccessor = dateTimeFormatter.parse(object.toString());
                    localTime = LocalTime.from(temporalAccessor);
                    instant = localTime.atDate(LocalDate.now()).toInstant(ZoneOffset.UTC);
                    break;
                case Timestamp.SCHEMA_NAME:
                    instant = Instant.ofEpochMilli(Long.parseLong(object.toString()));
                    break;
                case Time.SCHEMA_NAME:
                    localTime = LocalTime.ofSecondOfDay(Long.parseLong(object.toString()) / 1000);
                    instant = localTime.atDate(LocalDate.now()).toInstant(ZoneOffset.UTC);
                    break;
                case NanoTimestamp.SCHEMA_NAME:
                    instant = Instant.EPOCH.plus(Long.parseLong(object.toString()), ChronoUnit.NANOS);
                    break;
                case NanoTime.SCHEMA_NAME:
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
