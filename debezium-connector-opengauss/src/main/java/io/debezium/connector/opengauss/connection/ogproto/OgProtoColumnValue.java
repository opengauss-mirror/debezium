/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.opengauss.connection.ogproto;

import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;

import io.debezium.connector.opengauss.*;
import io.debezium.connector.opengauss.connection.AbstractColumnValue;
import io.debezium.connector.opengauss.connection.wal2json.DateTimeFormat;
import org.opengauss.geometric.PGpoint;
import org.opengauss.jdbc.PgArray;
import org.opengauss.util.PGmoney;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.opengauss.OgOid;
import io.debezium.connector.opengauss.OpengaussType;
import io.debezium.connector.opengauss.OpengaussValueConverter;
import io.debezium.connector.opengauss.TypeRegistry;
import io.debezium.connector.postgresql.proto.PgProto;
import io.debezium.data.SpecialValueDecimal;
import io.debezium.time.Conversions;

/**
 * Replication message column sent by <a href="https://github.com/debezium/postgres-decoderbufs">Postgres Decoderbufs</>
 *
 * @author Chris Cranford
 */
public class OgProtoColumnValue extends AbstractColumnValue<PgProto.DatumMessage> {

    private static final Logger LOGGER = LoggerFactory.getLogger(OgProtoColumnValue.class);

    /**
     * A number used by PostgreSQL to define minimum timestamp (inclusive).
     * Defined in timestamp.h
     */
    private static final long TIMESTAMP_MIN = -211813488000000000L;

    /**
     * A number used by PostgreSQL to define maximum timestamp (exclusive).
     * Defined in timestamp.h
     */
    private static final long TIMESTAMP_MAX = 9223371331200000000L;

    private PgProto.DatumMessage value;

    public OgProtoColumnValue(PgProto.DatumMessage value) {
        this.value = value;
    }

    @Override
    public PgProto.DatumMessage getRawValue() {
        return value;
    }

    @Override
    public boolean isNull() {
        return value.hasDatumMissing();
    }

    @Override
    public String asString() {
        if (value.hasDatumString()) {
            return value.getDatumString();
        }
        else if (value.hasDatumBytes()) {
            return new String(asByteArray(), Charset.forName("UTF-8"));
        }
        return null;
    }

    @Override
    public Boolean asBoolean() {
        if (value.hasDatumBool()) {
            return value.getDatumBool();
        }

        final String s = asString();
        if (s != null) {
            if (s.equalsIgnoreCase("t")) {
                return Boolean.TRUE;
            }
            else if (s.equalsIgnoreCase("f")) {
                return Boolean.FALSE;
            }
        }
        return null;
    }

    @Override
    public Integer asInteger() {
        if (value.hasDatumInt32()) {
            return value.getDatumInt32();
        }

        final String s = asString();
        return s != null ? Integer.valueOf(s) : null;
    }

    @Override
    public Long asLong() {
        if (value.hasDatumInt64()) {
            return value.getDatumInt64();
        }

        final String s = asString();
        return s != null ? Long.valueOf(s) : null;
    }

    @Override
    public Float asFloat() {
        if (value.hasDatumFloat()) {
            return value.getDatumFloat();
        }

        final String s = asString();
        return s != null ? Float.valueOf(s) : null;
    }

    @Override
    public Double asDouble() {
        if (value.hasDatumDouble()) {
            return value.getDatumDouble();
        }

        final String s = asString();
        return s != null ? Double.valueOf(s) : null;
    }

    @Override
    public Object asDecimal() {
        if (value.hasDatumDouble()) {
            return value.getDatumDouble();
        }

        final String s = asString();
        if (s != null) {
            return OpengaussValueConverter.toSpecialValue(s).orElseGet(() -> new SpecialValueDecimal(new BigDecimal(s)));
        }
        return null;
    }

    @Override
    public byte[] asByteArray() {
        return value.hasDatumBytes() ? value.getDatumBytes().toByteArray() : null;
    }

    @Override
    public LocalDate asLocalDate() {
        if (value.hasDatumInt32()) {
            return LocalDate.ofEpochDay(value.getDatumInt32());
        }

        final String s = asString();
        return s != null ? DateTimeFormat.get().date(s) : null;
    }

    @Override
    public Object asTime() {
        if (value.hasDatumInt64()) {
            return Duration.of(value.getDatumInt64(), ChronoUnit.MICROS);
        }

        final String s = asString();
        if (s != null) {
            return DateTimeFormat.get().time(s);
        }
        return null;
    }

    @Override
    public OffsetTime asOffsetTimeUtc() {
        if (value.hasDatumDouble()) {
            return Conversions.toInstantFromMicros((long) value.getDatumDouble()).atOffset(ZoneOffset.UTC).toOffsetTime();
        }

        final String s = asString();
        return s != null ? DateTimeFormat.get().timeWithTimeZone(s) : null;
    }

    @Override
    public OffsetDateTime asOffsetDateTimeAtUtc() {
        if (value.hasDatumInt64()) {
            if (value.getDatumInt64() >= TIMESTAMP_MAX) {
                LOGGER.trace("Infinite(+) value '{}' arrived from database", value.getDatumInt64());
                return OpengaussValueConverter.POSITIVE_INFINITY_OFFSET_DATE_TIME;
            }
            else if (value.getDatumInt64() < TIMESTAMP_MIN) {
                LOGGER.trace("Infinite(-) value '{}' arrived from database", value.getDatumInt64());
                return OpengaussValueConverter.NEGATIVE_INFINITY_OFFSET_DATE_TIME;
            }
            return Conversions.toInstantFromMicros(value.getDatumInt64()).atOffset(ZoneOffset.UTC);
        }

        final String s = asString();
        return s != null ? DateTimeFormat.get().timestampWithTimeZoneToOffsetDateTime(s).withOffsetSameInstant(ZoneOffset.UTC) : null;
    }

    @Override
    public Instant asInstant() {
        if (value.hasDatumInt64()) {
            if (value.getDatumInt64() >= TIMESTAMP_MAX) {
                LOGGER.trace("Infinite(+) value '{}' arrived from database", value.getDatumInt64());
                return OpengaussValueConverter.POSITIVE_INFINITY_INSTANT;
            }
            else if (value.getDatumInt64() < TIMESTAMP_MIN) {
                LOGGER.trace("Infinite(-) value '{}' arrived from database", value.getDatumInt64());
                return OpengaussValueConverter.NEGATIVE_INFINITY_INSTANT;
            }
            return Conversions.toInstantFromMicros(value.getDatumInt64());
        }

        final String s = asString();
        return s != null ? DateTimeFormat.get().timestampToInstant(asString()) : null;
    }

    @Override
    public Object asLocalTime() {
        return asTime();
    }

    @Override
    public Object asInterval() {
        if (value.hasDatumDouble()) {
            return value.getDatumDouble();
        }

        final String s = asString();
        return s != null ? super.asInterval() : null;
    }

    @Override
    public PGmoney asMoney() {
        if (value.hasDatumInt64()) {
            return new PGmoney(value.getDatumInt64() / 100.0);
        }
        return super.asMoney();
    }

    @Override
    public PGpoint asPoint() {
        if (value.hasDatumPoint()) {
            PgProto.Point datumPoint = value.getDatumPoint();
            return new PGpoint(datumPoint.getX(), datumPoint.getY());
        }
        else if (value.hasDatumBytes()) {
            return super.asPoint();
        }
        return null;
    }

    @Override
    public boolean isArray(OpengaussType type) {
        final int oidValue = type.getOid();
        switch (oidValue) {
            case OgOid.INT2_ARRAY:
            case OgOid.INT4_ARRAY:
            case OgOid.INT8_ARRAY:
            case OgOid.TEXT_ARRAY:
            case OgOid.NUMERIC_ARRAY:
            case OgOid.FLOAT4_ARRAY:
            case OgOid.FLOAT8_ARRAY:
            case OgOid.BOOL_ARRAY:
            case OgOid.DATE_ARRAY:
            case OgOid.TIME_ARRAY:
            case OgOid.TIMETZ_ARRAY:
            case OgOid.TIMESTAMP_ARRAY:
            case OgOid.TIMESTAMPTZ_ARRAY:
            case OgOid.BYTEA_ARRAY:
            case OgOid.VARCHAR_ARRAY:
            case OgOid.OID_ARRAY:
            case OgOid.BPCHAR_ARRAY:
            case OgOid.MONEY_ARRAY:
            case OgOid.NAME_ARRAY:
            case OgOid.INTERVAL_ARRAY:
            case OgOid.CHAR_ARRAY:
            case OgOid.VARBIT_ARRAY:
            case OgOid.UUID_ARRAY:
            case OgOid.XML_ARRAY:
            case OgOid.POINT_ARRAY:
            case OgOid.JSONB_ARRAY:
            case OgOid.JSON_ARRAY:
            case OgOid.REF_CURSOR_ARRAY:
            case OgOid.INET_ARRAY:
            case OgOid.CIDR_ARRAY:
            case OgOid.MACADDR_ARRAY:
            case OgOid.MACADDR8_ARRAY:
            case OgOid.TSRANGE_ARRAY:
            case OgOid.TSTZRANGE_ARRAY:
            case OgOid.DATERANGE_ARRAY:
            case OgOid.INT4RANGE_ARRAY:
            case OgOid.NUM_RANGE_ARRAY:
            case OgOid.INT8RANGE_ARRAY:
                return true;
            default:
                return type.isArrayType();
        }
    }

    @Override
    public Object asArray(String columnName, OpengaussType type, String fullType, OpengaussStreamingChangeEventSource.PgConnectionSupplier connection) {
        // Currently the logical decoding plugin sends unhandled types as a byte array containing the string
        // representation (in Postgres) of the array value.
        // The approach to decode this is sub-optimal but the only way to improve this is to update the plugin.
        // Reasons for it being sub-optimal include:
        // 1. It requires a Postgres JDBC connection to deserialize
        // 2. The byte-array is a serialised string but we make the assumption its UTF-8 encoded (which it will
        // be in most cases)
        // 3. For larger arrays and especially 64-bit integers and the like it is less efficient sending string
        // representations over the wire.
        try {
            byte[] data = asByteArray();
            if (data == null) {
                return null;
            }
            String dataString = new String(data, Charset.forName("UTF-8"));
            PgArray arrayData = new PgArray(connection.get(), (int) value.getColumnType(), dataString);
            Object deserializedArray = arrayData.getArray();
            return Arrays.asList((Object[]) deserializedArray);
        }
        catch (SQLException e) {
            LOGGER.warn("Unexpected exception trying to process PgArray column '{}'", value.getColumnName(), e);
        }
        return null;
    }

    @Override
    public Object asDefault(TypeRegistry typeRegistry, int columnType, String columnName, String fullType, boolean includeUnknownDatatypes,
                            OpengaussStreamingChangeEventSource.PgConnectionSupplier connection) {
        final OpengaussType type = typeRegistry.get(columnType);
        if (type.getOid() == typeRegistry.geometryOid() ||
                type.getOid() == typeRegistry.geographyOid() ||
                type.getOid() == typeRegistry.citextOid() ||
                type.getOid() == typeRegistry.hstoreOid()) {
            return asByteArray();
        }

        // unknown data type is sent by decoder as binary value
        if (includeUnknownDatatypes) {
            return asByteArray();
        }

        return null;
    }
}
