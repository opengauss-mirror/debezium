/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.opengauss.connection;

import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;

import io.debezium.connector.opengauss.OpengaussStreamingChangeEventSource;
import io.debezium.connector.opengauss.OpengaussType;
import io.debezium.connector.opengauss.OpengaussValueConverter;
import io.debezium.connector.opengauss.TypeRegistry;
import org.apache.kafka.connect.errors.ConnectException;
import org.opengauss.geometric.PGbox;
import org.opengauss.geometric.PGcircle;
import org.opengauss.geometric.PGline;
import org.opengauss.geometric.PGlseg;
import org.opengauss.geometric.PGpath;
import org.opengauss.geometric.PGpoint;
import org.opengauss.geometric.PGpolygon;
import org.opengauss.jdbc.PgArray;
import org.opengauss.util.PGInterval;
import org.opengauss.util.PGmoney;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.opengauss.connection.wal2json.DateTimeFormat;
import io.debezium.enums.ErrorCode;

/**
 * @author Chris Cranford
 */
public abstract class AbstractColumnValue<T> implements ReplicationMessage.ColumnValue<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractColumnValue.class);

    @Override
    public LocalDate asLocalDate() {
        return DateTimeFormat.get().date(asString());
    }

    @Override
    public Object asTime() {
        return asString();
    }

    @Override
    public Object asLocalTime() {
        return DateTimeFormat.get().time(asString());
    }

    @Override
    public OffsetTime asOffsetTimeUtc() {
        return DateTimeFormat.get().timeWithTimeZone(asString());
    }

    @Override
    public OffsetDateTime asOffsetDateTimeAtUtc() {
        if ("infinity".equals(asString())) {
            return OpengaussValueConverter.POSITIVE_INFINITY_OFFSET_DATE_TIME;
        }
        else if ("-infinity".equals(asString())) {
            return OpengaussValueConverter.NEGATIVE_INFINITY_OFFSET_DATE_TIME;
        }
        return DateTimeFormat.get().timestampWithTimeZoneToOffsetDateTime(asString()).withOffsetSameInstant(ZoneOffset.UTC);
    }

    @Override
    public Instant asInstant() {
        if ("infinity".equals(asString())) {
            return OpengaussValueConverter.POSITIVE_INFINITY_INSTANT;
        }
        else if ("-infinity".equals(asString())) {
            return OpengaussValueConverter.NEGATIVE_INFINITY_INSTANT;
        }
        return DateTimeFormat.get().timestampToInstant(asString());
    }

    @Override
    public PGbox asBox() {
        try {
            return new PGbox(asString());
        }
        catch (final SQLException e) {
            LOGGER.error("{}Failed to parse point {}, {}", ErrorCode.DATA_CONVERT_EXCEPTION, asString(),
                e.getMessage());
            throw new ConnectException(e);
        }
    }

    @Override
    public PGcircle asCircle() {
        try {
            return new PGcircle(asString());
        }
        catch (final SQLException e) {
            LOGGER.error("{}Failed to parse circle {}, {}", ErrorCode.DATA_CONVERT_EXCEPTION, asString(),
                e.getMessage());
            throw new ConnectException(e);
        }
    }

    @Override
    public Object asInterval() {
        try {
            return new PGInterval(asString());
        }
        catch (final SQLException e) {
            LOGGER.error("{}Failed to parse point {}, {}", ErrorCode.DATA_CONVERT_EXCEPTION, asString(),
                e.getMessage());
            throw new ConnectException(e);
        }
    }

    @Override
    public PGline asLine() {
        try {
            return new PGline(asString());
        }
        catch (final SQLException e) {
            LOGGER.error("{}Failed to parse point {}, {}", ErrorCode.DATA_CONVERT_EXCEPTION, asString(),
                e.getMessage());
            throw new ConnectException(e);
        }
    }

    @Override
    public PGlseg asLseg() {
        try {
            return new PGlseg(asString());
        }
        catch (final SQLException e) {
            LOGGER.error("{}Failed to parse point {}, {}", ErrorCode.DATA_CONVERT_EXCEPTION, asString(),
                e.getMessage());
            throw new ConnectException(e);
        }
    }

    @Override
    public PGmoney asMoney() {
        try {
            final String value = asString();
            if (value != null && value.startsWith("-")) {
                final String negativeMoney = "(" + value.substring(1) + ")";
                return new PGmoney(negativeMoney);
            }
            return new PGmoney(asString());
        }
        catch (final SQLException e) {
            LOGGER.error("{}Failed to parse money {}, {}", ErrorCode.DATA_CONVERT_EXCEPTION, asString(),
                e.getMessage());
            throw new ConnectException(e);
        }
    }

    @Override
    public PGpath asPath() {
        try {
            return new PGpath(asString());
        }
        catch (final SQLException e) {
            LOGGER.error("{}Failed to parse point {}, {}", ErrorCode.DATA_CONVERT_EXCEPTION, asString(),
                e.getMessage());
            throw new ConnectException(e);
        }
    }

    @Override
    public PGpoint asPoint() {
        try {
            return new PGpoint(asString());
        }
        catch (final SQLException e) {
            LOGGER.error("{}Failed to parse point {}, {}", ErrorCode.DATA_CONVERT_EXCEPTION, asString(),
                e.getMessage());
            throw new ConnectException(e);
        }
    }

    @Override
    public PGpolygon asPolygon() {
        try {
            return new PGpolygon(asString());
        }
        catch (final SQLException e) {
            LOGGER.error("{}Failed to parse point {}, {}", ErrorCode.DATA_CONVERT_EXCEPTION, asString(),
                e.getMessage());
            throw new ConnectException(e);
        }
    }

    @Override
    public boolean isArray(OpengaussType type) {
        return type.isArrayType();
    }

    @Override
    public Object asArray(String columnName, OpengaussType type, String fullType, OpengaussStreamingChangeEventSource.PgConnectionSupplier connection) {
        try {
            final String dataString = asString();
            return new PgArray(connection.get(), type.getOid(), dataString);
        }
        catch (SQLException e) {
            LOGGER.warn("{}Unexpected exception trying to process PgArray ({}) column '{}', {}",
                ErrorCode.DATA_CONVERT_EXCEPTION, fullType, columnName, e.getMessage());
        }
        return null;
    }

    @Override
    public Object asDefault(TypeRegistry typeRegistry, int columnType, String columnName, String fullType, boolean includeUnknownDatatypes,
                            OpengaussStreamingChangeEventSource.PgConnectionSupplier connection) {
        if (includeUnknownDatatypes) {
            // this includes things like PostGIS geoemetries or other custom types
            // leave up to the downstream message recipient to deal with
            LOGGER.debug("processing column '{}' with unknown data type '{}' as byte array", columnName, fullType);
            return asString();
        }
        LOGGER.debug("Unknown column type {} for column {} – ignoring", fullType, columnName);
        return null;
    }
}
