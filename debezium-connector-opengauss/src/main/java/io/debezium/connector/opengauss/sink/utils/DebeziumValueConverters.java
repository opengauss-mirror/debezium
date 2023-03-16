/**
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.opengauss.sink.utils;

import io.debezium.connector.opengauss.sink.object.ColumnMetaData;
import io.debezium.data.geometry.Geometry;
import io.debezium.data.geometry.Point;
import io.debezium.util.HexConverter;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;

import java.time.Instant;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAccessor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Description: ValueConverter class
 * @author wangzhengyuan
 * @date 2023/01/17
 */
public final class DebeziumValueConverters {
    private DebeziumValueConverters(){}

    private static HashMap<String, ValueConverter> dataTypeConverterMap = new HashMap<String, ValueConverter>() {
        {
            put("integer", (columnName, value) -> convertNumberType(columnName, value));
            put("tinyint", (columnName, value) -> convertNumberType(columnName, value));
            put("double", (columnName, value) -> convertNumberType(columnName, value));
            put("float", (columnName, value) -> convertNumberType(columnName, value));
            put("tinyblob", (columnName, value) -> convertBlob(columnName, value));
            put("mediumblob", (columnName, value) -> convertBlob(columnName, value));
            put("blob", (columnName, value) -> convertBlob(columnName, value));
            put("longblob", (columnName, value) -> convertBlob(columnName, value));
            put("datetime", (columnName, value) -> convertDatetimeAndTimestamp(columnName, value));
            put("timestamp", (columnName, value) -> convertDatetimeAndTimestamp(columnName, value));
            put("date", (columnName, value) -> convertDate(columnName, value));
            put("time", (columnName, value) -> convertTime(columnName, value));
            put("binary", (columnName, value) -> convertBinary(columnName, value));
            put("varbinary", (columnName, value) -> convertBinary(columnName, value));
            put("bit", (columnName, value) -> convertBit(columnName, value));
            put("set", (columnName, value) -> convertSet(columnName, value));
            put("point", (columnName, value) -> convertPoint(columnName, value));
            put("geometry", (columnName, value) -> convertPoint(columnName, value));
            put("linestring", (columnName, value) -> convertLinestring(columnName, value));
            put("polygon", (columnName, value) -> convertPolygon(columnName, value));
            put("multipoint", (columnName, value) -> convertMultipoint(columnName, value));
            put("multilinestring", (columnName, value) -> convertMultilinestring(columnName, value));
            put("multipolygon", (columnName, value) -> convertMultipolygon(columnName, value));
            put("geometrycollection", (columnName, value) -> convertGeometrycollection(columnName, value));
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
        Object object = value.get(columnName);
        return object == null ? null : addingSingleQuotation(object.toString());
    }

    private static String convertNumberType(String columnName, Struct valueStruct) {
        Object object = valueStruct.get(columnName);
        if (object != null){
            return object.toString();
        }
        return null;
    }

    private static String convertBlob(String columnName, Struct valueStruct) {
        byte[] bytes = valueStruct.getBytes(columnName);
        if (bytes != null) {
            String hexString = new String(bytes);
            byte[] indexes = parseHexStr2bytes(hexString);
            return addingSingleQuotation(new String(indexes));
        }
        return null;
    }

    private static String convertDatetimeAndTimestamp(String columnName, Struct valueStruct) {
        Field field = valueStruct.schema().field(columnName);
        String schemaName = field.schema().name();
        Object object = valueStruct.get(columnName);
        if (object == null){
            return null;
        }
        Instant instant = convertDbzDateTime(object, schemaName);
        DateTimeFormatter dateTimeFormatter;
        if ("io.debezium.time.ZonedTimestamp".equals(schemaName)){
            dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS").withZone(ZoneId.of("Asia/Shanghai"));
        } else {
            dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS").withZone(ZoneOffset.UTC);
        }
        return addingSingleQuotation(dateTimeFormatter.format(instant));
    }

    private static String convertDate(String columnName, Struct valueStruct) {
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

    private static String convertTime(String columnName, Struct valueStruct) {
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

    private static String convertBinary(String columnName, Struct value){
        String hexString = convertBinaryToHex(columnName, value);
        return hexString == null ? null : addingSingleQuotation(new String(Objects.requireNonNull(parseHexStr2bytes(hexString))));
    }

    private static String convertBinaryToHex(String columnName, Struct value) {
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

    private static String convertBit(String columnName, Struct value) {
        Field field = value.schema().field(columnName);
        String schemaName = field.schema().name();
        byte[] bytes;
        if ("io.debezium.data.Bits".equals(schemaName)) {
            bytes = value.getBytes(columnName);
            return bytes == null ? null : "b" + convertBitString(bytes);
        } else {
            return value.getBoolean(columnName) + "";
        }
    }

    private static String convertBitString(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        sb.append(Integer.toBinaryString(adjustByte(bytes[bytes.length - 1])));
        if (bytes.length > 1) {
            for (int i = bytes.length - 2; i >= 0 ; i--) {
                sb.append(Integer.toBinaryString((bytes[i] & 0xFF) + 0x100).substring(1));
            }
        }
        return addingSingleQuotation(sb.toString());
    }

    private static int adjustByte(byte abyte) {
        return abyte >= 0 ? abyte : abyte + 256;
    }

    private static String convertSet(String columnName, Struct valueStruct) {
        byte[] bytes = valueStruct.getBytes(columnName);
        return bytes == null ? null : addingSingleQuotation(new String(bytes));
    }

    private static String convertPoint(String columnName, Struct value) {
        // openGauss point -> mysql geometry,point
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

    private static String formatPoint(double[] coordinate) {
        return "ST_GeomFROMtEXT('POINT(" + coordinate[0] + " " + coordinate[1] + ")')";
    }

    private static String convertLinestring(String columnName, Struct valueStruct) {
        byte[] bytes = valueStruct.getBytes(columnName);
        if (bytes == null) {
            return null;
        }
        String[] coordinateArr = getCoordinate(bytes);
        return formatLinestring(coordinateArr);
    }

    private static String formatLinestring(String[] coordinateArr) {
        return "ST_GeomFROMtEXT('LINESTRING(" + formatCoordinate(coordinateArr) + ")')";
    }

    private static String convertPolygon(String columnName, Struct valueStruct) {
        byte[] bytes = valueStruct.getBytes(columnName);
        if (bytes == null) {
            return null;
        }
        String[] coordinateArr = getCoordinate(bytes);
        return formatPolygon(coordinateArr);
    }

    private static String formatPolygon(String[] coordinateArr) {
        return "ST_GeomFROMtEXT('POLYGON((" + formatCoordinate(coordinateArr) + "))')";
    }

    private static String convertMultipoint(String columnName, Struct valueStruct) {
        byte[] bytes = valueStruct.getBytes(columnName);
        if (bytes == null) {
            return null;
        }
        String[] coordinateArr = getCoordinate(bytes);
        return formatMultipoint(coordinateArr);
    }

    private static String formatMultipoint(String[] coordinateArr) {
        return "ST_GeomFROMtEXT('MULTIPOINT(" + formatCoordinate(coordinateArr) + ")')";
    }

    private static String convertMultilinestring(String columnName, Struct valueStruct) {
        byte[] bytes = valueStruct.getBytes(columnName);
        if (bytes == null) {
            return null;
        }
        return formatMutlinestring(new String(bytes).replaceAll("\\s*", ""));
    }

    private static String formatMutlinestring(String originalString) {
        StringBuilder sb = new StringBuilder();
        sb.append(originalString);
        for (int i = 0; i < originalString.length(); i++) {
            if (i > 0 && originalString.charAt(i) == ',' && originalString.charAt(i - 1) != ')' && originalString.charAt(i - 1) != ']') {
                sb.setCharAt(i, ' ');
            }
        }
        originalString = sb.toString();
        originalString = originalString.replace("(", "");
        originalString = originalString.replace(")", "");
        originalString = originalString.replace('[', '(');
        originalString = originalString.replace(']', ')');
        return "ST_GeomFROMtEXT('MULTILINESTRING(" + originalString + ")')";
    }

    private static String convertMultipolygon(String columnName, Struct valueStruct) {
        byte[] bytes = valueStruct.getBytes(columnName);
        if (bytes == null) {
            return null;
        }
        return formatMutipolygon(new String(bytes).replaceAll("\\s*", ""));
    }

    private static String formatMutipolygon(String originalString) {
        StringBuilder sb = new StringBuilder();
        sb.append(originalString);
        for (int i = 0; i < originalString.length(); i++) {
            if (i > 0 && originalString.charAt(i) == ',' && originalString.charAt(i - 1) != ')' && originalString.charAt(i - 1) != ']') {
                sb.setCharAt(i, ' ');
            }
        }
        originalString = sb.toString();
        originalString = originalString.replace("(", "");
        originalString = originalString.replace(")", "");
        originalString = originalString.replace("[","((");
        originalString = originalString.replace("]", "))");
        return "ST_GeomFROMtEXT('MULTIPOLYGON(" + originalString + ")')";
    }

    private static String convertGeometrycollection(String columnName, Struct valueStruct) {
        byte[] bytes = valueStruct.getBytes(columnName);
        if (bytes == null) {
            return null;
        }
        return formatGeometrycollection(new String(bytes));
    }

    private static String formatGeometrycollection(String originalString) {
        return "ST_GeomFROMtEXT('GEOMETRYCOLLECTION(" + originalString + ")')";
    }

    private static String[] getCoordinate(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        List<String> list = new ArrayList<>();
        for (byte aByte : bytes) {
            if (aByte == 46 || (aByte >= 48 && aByte <= 57)) {
                sb.append((char)aByte);
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
        sb = new StringBuilder(sb.substring(0,sb.lastIndexOf(",")));
        return sb.toString();
    }

    private static String addingSingleQuotation(Object originValue) {
        return "'" + originValue.toString() + "'";
    }
}
