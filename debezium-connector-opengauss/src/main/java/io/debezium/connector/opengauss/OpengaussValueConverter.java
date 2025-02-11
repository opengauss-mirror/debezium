/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.opengauss;

import static java.time.ZoneId.systemDefault;

import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.Charset;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.errors.ConnectException;
import org.opengauss.PGStatement;
import org.opengauss.geometric.PGpoint;
import org.opengauss.jdbc.PgArray;
import org.opengauss.util.HStoreConverter;
import org.opengauss.util.PGInterval;
import org.opengauss.util.PGobject;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;

import io.debezium.config.CommonConnectorConfig.BinaryHandlingMode;
import io.debezium.connector.opengauss.OpengaussConnectorConfig.HStoreHandlingMode;
import io.debezium.connector.opengauss.OpengaussConnectorConfig.IntervalHandlingMode;
import io.debezium.connector.opengauss.data.Ltree;
import io.debezium.connector.postgresql.proto.PgProto;
import io.debezium.data.Bits;
import io.debezium.data.Json;
import io.debezium.data.SpecialValueDecimal;
import io.debezium.data.Uuid;
import io.debezium.data.VariableScaleDecimal;
import io.debezium.data.geometry.Geography;
import io.debezium.data.geometry.Geometry;
import io.debezium.data.geometry.Point;
import io.debezium.enums.ErrorCode;
import io.debezium.jdbc.JdbcValueConverters;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.relational.Column;
import io.debezium.relational.ValueConverter;
import io.debezium.time.Conversions;
import io.debezium.time.Interval;
import io.debezium.time.MicroDuration;
import io.debezium.time.MicroTime;
import io.debezium.time.MicroTimestamp;
import io.debezium.time.NanoTime;
import io.debezium.time.ZonedTime;
import io.debezium.time.ZonedTimestamp;
import io.debezium.util.NumberConversions;
import io.debezium.util.Strings;

/**
 * A provider of {@link ValueConverter}s and {@link SchemaBuilder}s for various Postgres specific column types.
 *
 * In addition to handling data type conversion from values coming from JDBC, this is also expected to handle data type
 * conversion for data types coming from the logical decoding plugin.
 *
 * @author Horia Chiorean (hchiorea@redhat.com)
 */
public class OpengaussValueConverter extends JdbcValueConverters {

    public static final Timestamp POSITIVE_INFINITY_TIMESTAMP = new Timestamp(PGStatement.DATE_POSITIVE_INFINITY);
    public static final Instant POSITIVE_INFINITY_INSTANT = Conversions.toInstantFromMicros(PGStatement.DATE_POSITIVE_INFINITY);
    public static final LocalDateTime POSITIVE_INFINITY_LOCAL_DATE_TIME = LocalDateTime.ofInstant(POSITIVE_INFINITY_INSTANT, ZoneOffset.UTC);
    public static final OffsetDateTime POSITIVE_INFINITY_OFFSET_DATE_TIME = OffsetDateTime.ofInstant(Conversions.toInstantFromMillis(PGStatement.DATE_POSITIVE_INFINITY),
            ZoneOffset.UTC);

    public static final Timestamp NEGATIVE_INFINITY_TIMESTAMP = new Timestamp(PGStatement.DATE_NEGATIVE_INFINITY);
    public static final Instant NEGATIVE_INFINITY_INSTANT = Conversions.toInstantFromMicros(PGStatement.DATE_NEGATIVE_INFINITY);
    public static final LocalDateTime NEGATIVE_INFINITY_LOCAL_DATE_TIME = LocalDateTime.ofInstant(NEGATIVE_INFINITY_INSTANT, ZoneOffset.UTC);
    public static final OffsetDateTime NEGATIVE_INFINITY_OFFSET_DATE_TIME = OffsetDateTime.ofInstant(Conversions.toInstantFromMillis(PGStatement.DATE_NEGATIVE_INFINITY),
            ZoneOffset.UTC);

    /**
     * Variable scale decimal/numeric is defined by metadata
     * scale - 0
     * length - 131089
     */
    private static final int VARIABLE_SCALE_DECIMAL_LENGTH = 131089;

    /**
     * A string denoting not-a- number for FP and Numeric types
     */
    public static final String N_A_N = "NaN";

    /**
     * A string denoting positive infinity for FP and Numeric types
     */
    public static final String POSITIVE_INFINITY = "Infinity";

    /**
     * A string denoting negative infinity for FP and Numeric types
     */
    public static final String NEGATIVE_INFINITY = "-Infinity";

    private static final BigDecimal MICROSECONDS_PER_SECOND = new BigDecimal(1_000_000);

    /**
     * A formatter used to parse TIMETZ columns when provided as strings.
     */
    private static final DateTimeFormatter TIME_WITH_TIMEZONE_FORMATTER = new DateTimeFormatterBuilder()
            .appendPattern("HH:mm:ss")
            .appendFraction(ChronoField.MICRO_OF_SECOND, 0, 6, true)
            .appendPattern("[XXX][XX][X]")
            .toFormatter();

    /**
     * {@code true} if fields of data type not know should be handle as opaque binary;
     * {@code false} if they should be omitted
     */
    private final boolean includeUnknownDatatypes;

    private final TypeRegistry typeRegistry;
    private final HStoreHandlingMode hStoreMode;
    private final IntervalHandlingMode intervalMode;

    /**
     * The current database's character encoding.
     */
    private final Charset databaseCharset;

    private final JsonFactory jsonFactory;

    private final String toastPlaceholderString;
    private final byte[] toastPlaceholderBinary;

    private final int moneyFractionDigits;

    public static OpengaussValueConverter of(OpengaussConnectorConfig connectorConfig, Charset databaseCharset, TypeRegistry typeRegistry) {
        return new OpengaussValueConverter(
                databaseCharset,
                connectorConfig.getDecimalMode(),
                connectorConfig.getTemporalPrecisionMode(),
                ZoneOffset.UTC,
                null,
                connectorConfig.includeUnknownDatatypes(),
                typeRegistry,
                connectorConfig.hStoreHandlingMode(),
                connectorConfig.binaryHandlingMode(),
                connectorConfig.intervalHandlingMode(),
                connectorConfig.getUnavailableValuePlaceholder(),
                connectorConfig.moneyFractionDigits());
    }

    protected OpengaussValueConverter(Charset databaseCharset, DecimalMode decimalMode,
                                      TemporalPrecisionMode temporalPrecisionMode, ZoneOffset defaultOffset,
                                      BigIntUnsignedMode bigIntUnsignedMode, boolean includeUnknownDatatypes, TypeRegistry typeRegistry,
                                      HStoreHandlingMode hStoreMode, BinaryHandlingMode binaryMode, IntervalHandlingMode intervalMode,
                                      byte[] toastPlaceholder, int moneyFractionDigits) {
        super(decimalMode, temporalPrecisionMode, defaultOffset, null, bigIntUnsignedMode, binaryMode);
        this.databaseCharset = databaseCharset;
        this.jsonFactory = new JsonFactory();
        this.includeUnknownDatatypes = includeUnknownDatatypes;
        this.typeRegistry = typeRegistry;
        this.hStoreMode = hStoreMode;
        this.intervalMode = intervalMode;
        this.toastPlaceholderBinary = toastPlaceholder;
        this.toastPlaceholderString = new String(toastPlaceholder);
        this.moneyFractionDigits = moneyFractionDigits;
    }

    @Override
    public SchemaBuilder schemaBuilder(Column column) {
        int oidValue = column.nativeType();
        switch (oidValue) {
            case OgOid.BIT:
            case OgOid.BIT_ARRAY:
            case OgOid.VARBIT:
                return column.length() > 1 ? Bits.builder(column.length()) : SchemaBuilder.bool();
            case OgOid.INTERVAL:
                return intervalMode == IntervalHandlingMode.STRING ? Interval.builder() : MicroDuration.builder();
            case OgOid.TIMESTAMPTZ:
                // JDBC reports this as "timestamp" even though it's with tz, so we can't use the base class...
                return ZonedTimestamp.builder();
            case OgOid.TIMETZ:
                // JDBC reports this as "time" but this contains TZ information
                return ZonedTime.builder();
            case OgOid.OID:
                return SchemaBuilder.int64();
            case OgOid.JSONB_OID:
            case OgOid.JSON:
                return Json.builder();
            case OgOid.TSRANGE_OID:
            case OgOid.TSTZRANGE_OID:
            case OgOid.DATERANGE_OID:
            case OgOid.INET_OID:
            case OgOid.CIDR_OID:
            case OgOid.MACADDR_OID:
            case OgOid.MACADDR8_OID:
            case OgOid.INT4RANGE_OID:
            case OgOid.NUM_RANGE_OID:
            case OgOid.INT8RANGE_OID:
                return SchemaBuilder.string();
            case OgOid.UUID:
                return Uuid.builder();
            case OgOid.POINT:
                return Point.builder();
            case OgOid.MONEY:
                return moneySchema();
            case OgOid.NUMERIC:
                return numericSchema(column);
            case OgOid.INT1:
                // support unsigned byte
                return SchemaBuilder.int16();
            case OgOid.BYTEA:
                return binaryMode.getSchema();
            case OgOid.INT2_ARRAY:
                return SchemaBuilder.array(SchemaBuilder.OPTIONAL_INT16_SCHEMA);
            case OgOid.INT4_ARRAY:
                return SchemaBuilder.array(SchemaBuilder.OPTIONAL_INT32_SCHEMA);
            case OgOid.INT8_ARRAY:
                return SchemaBuilder.array(SchemaBuilder.OPTIONAL_INT64_SCHEMA);
            case OgOid.CHAR_ARRAY:
            case OgOid.VARCHAR_ARRAY:
            case OgOid.TEXT_ARRAY:
            case OgOid.BPCHAR_ARRAY:
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
                return SchemaBuilder.array(SchemaBuilder.OPTIONAL_STRING_SCHEMA);
            case OgOid.NUMERIC_ARRAY:
                return SchemaBuilder.array(numericSchema(column).optional().build());
            case OgOid.FLOAT4_ARRAY:
                return SchemaBuilder.array(Schema.OPTIONAL_FLOAT32_SCHEMA);
            case OgOid.FLOAT8_ARRAY:
                return SchemaBuilder.array(Schema.OPTIONAL_FLOAT64_SCHEMA);
            case OgOid.BOOL_ARRAY:
                return SchemaBuilder.array(SchemaBuilder.OPTIONAL_BOOLEAN_SCHEMA);
            case OgOid.DATE_ARRAY:
                if (adaptiveTimePrecisionMode || adaptiveTimeMicrosecondsPrecisionMode) {
                    return SchemaBuilder.array(io.debezium.time.Date.builder().optional().build());
                }
                return SchemaBuilder.array(org.apache.kafka.connect.data.Date.builder().optional().build());
            case OgOid.UUID_ARRAY:
                return SchemaBuilder.array(Uuid.builder().optional().build());
            case OgOid.JSONB_ARRAY:
            case OgOid.JSON_ARRAY:
                return SchemaBuilder.array(Json.builder().optional().build());
            case OgOid.TIME_ARRAY:
                return SchemaBuilder.array(MicroTime.builder().optional().build());
            case OgOid.TIMETZ_ARRAY:
                return SchemaBuilder.array(ZonedTime.builder().optional().build());
            case OgOid.TIMESTAMP_ARRAY:
                if (adaptiveTimePrecisionMode || adaptiveTimeMicrosecondsPrecisionMode) {
                    if (getTimePrecision(column) <= 3) {
                        return SchemaBuilder.array(io.debezium.time.Timestamp.builder().optional().build());
                    }
                    if (getTimePrecision(column) <= 6) {
                        return SchemaBuilder.array(MicroTimestamp.builder().optional().build());
                    }
                    return SchemaBuilder.array(NanoTime.builder().optional().build());
                }
                return SchemaBuilder.array(org.apache.kafka.connect.data.Timestamp.builder().optional().build());
            case OgOid.TIMESTAMPTZ_ARRAY:
                return SchemaBuilder.array(ZonedTimestamp.builder().optional().build());
            case OgOid.OID_ARRAY:
                return SchemaBuilder.array(SchemaBuilder.OPTIONAL_INT64_SCHEMA);
            case OgOid.BYTEA_ARRAY:
            case OgOid.MONEY_ARRAY:
            case OgOid.NAME_ARRAY:
            case OgOid.INTERVAL_ARRAY:
            case OgOid.VARBIT_ARRAY:
            case OgOid.XML_ARRAY:
            case OgOid.POINT_ARRAY:
            case OgOid.REF_CURSOR_ARRAY:
                // These array types still need to be implemented. The superclass won't handle them so
                // we return null here until we can code schema implementations for them.
                return null;

            default:
                if (oidValue == typeRegistry.geometryOid()) {
                    return Geometry.builder();
                }
                else if (oidValue == typeRegistry.geographyOid()) {
                    return Geography.builder();
                }
                else if (oidValue == typeRegistry.citextOid()) {
                    return SchemaBuilder.string();
                }
                else if (oidValue == typeRegistry.geometryArrayOid()) {
                    return SchemaBuilder.array(Geometry.builder().optional().build());
                }
                else if (oidValue == typeRegistry.hstoreOid()) {
                    return hstoreSchema();
                }
                else if (oidValue == typeRegistry.ltreeOid()) {
                    return Ltree.builder();
                }
                else if (oidValue == typeRegistry.hstoreArrayOid()) {
                    return SchemaBuilder.array(hstoreSchema().optional().build());
                }
                else if (oidValue == typeRegistry.geographyArrayOid()) {
                    return SchemaBuilder.array(Geography.builder().optional().build());
                }
                else if (oidValue == typeRegistry.citextArrayOid()) {
                    return SchemaBuilder.array(SchemaBuilder.OPTIONAL_STRING_SCHEMA);
                }
                else if (oidValue == typeRegistry.ltreeArrayOid()) {
                    return SchemaBuilder.array(Ltree.builder().optional().build());
                }

                final OpengaussType resolvedType = typeRegistry.get(oidValue);
                if (resolvedType.isEnumType()) {
                    return io.debezium.data.Enum.builder(Strings.join(",", resolvedType.getEnumValues()));
                }
                else if (resolvedType.isArrayType() && resolvedType.getElementType().isEnumType()) {
                    List<String> enumValues = resolvedType.getElementType().getEnumValues();
                    return SchemaBuilder.array(io.debezium.data.Enum.builder(Strings.join(",", enumValues)));
                }

                final SchemaBuilder jdbcSchemaBuilder = super.schemaBuilder(column);
                if (jdbcSchemaBuilder == null) {
                    return includeUnknownDatatypes ? binaryMode.getSchema() : null;
                }
                else {
                    return jdbcSchemaBuilder;
                }
        }
    }

    private SchemaBuilder numericSchema(Column column) {
        if (decimalMode == DecimalMode.PRECISE && isVariableScaleDecimal(column)) {
            return VariableScaleDecimal.builder();
        }
        return SpecialValueDecimal.builder(decimalMode, column.length(), column.scale().orElseGet(() -> 0));
    }

    private SchemaBuilder hstoreSchema() {
        if (hStoreMode == OpengaussConnectorConfig.HStoreHandlingMode.JSON) {
            return Json.builder();
        }
        else {
            // keys are not nullable, but values are
            return SchemaBuilder.map(
                    SchemaBuilder.STRING_SCHEMA,
                    SchemaBuilder.OPTIONAL_STRING_SCHEMA);
        }
    }

    private SchemaBuilder moneySchema() {
        switch (decimalMode) {
            case DOUBLE:
                return SchemaBuilder.float64();
            case PRECISE:
                return Decimal.builder(moneyFractionDigits);
            case STRING:
                return SchemaBuilder.string();
            default:
                throw new IllegalArgumentException("Unknown decimalMode");
        }
    }

    @Override
    public ValueConverter converter(Column column, Field fieldDefn) {
        int oidValue = column.nativeType();
        switch (oidValue) {
            case OgOid.BIT:
            case OgOid.VARBIT:
                return convertBits(column, fieldDefn);
            case OgOid.INTERVAL:
                return data -> convertInterval(column, fieldDefn, data);
            case OgOid.TIME:
                return data -> convertTime(column, fieldDefn, data);
            case OgOid.TIMESTAMP:
                return ((ValueConverter) (data -> convertTimestampToLocalDateTime(column, fieldDefn, data))).and(super.converter(column, fieldDefn));
            case OgOid.TIMESTAMPTZ:
                return data -> convertTimestampWithZone(column, fieldDefn, data);
            case OgOid.TIMETZ:
                return data -> convertTimeWithZone(column, fieldDefn, data);
            case OgOid.OID:
                return data -> convertBigInt(column, fieldDefn, data);
            case OgOid.JSONB_OID:
            case OgOid.UUID:
            case OgOid.TSRANGE_OID:
            case OgOid.TSTZRANGE_OID:
            case OgOid.DATERANGE_OID:
            case OgOid.JSON:
            case OgOid.INET_OID:
            case OgOid.CIDR_OID:
            case OgOid.MACADDR_OID:
            case OgOid.MACADDR8_OID:
            case OgOid.INT4RANGE_OID:
            case OgOid.NUM_RANGE_OID:
            case OgOid.INT8RANGE_OID:
                return data -> convertString(column, fieldDefn, data);
            case OgOid.POINT:
                return data -> convertPoint(column, fieldDefn, data);
            case OgOid.MONEY:
                return data -> convertMoney(column, fieldDefn, data, decimalMode);
            case OgOid.NUMERIC:
                return (data) -> convertDecimal(column, fieldDefn, data, decimalMode);
            case OgOid.BYTEA:
                return data -> convertBinary(column, fieldDefn, data, binaryMode);
            case OgOid.INT2_ARRAY:
            case OgOid.INT4_ARRAY:
            case OgOid.INT8_ARRAY:
            case OgOid.CHAR_ARRAY:
            case OgOid.VARCHAR_ARRAY:
            case OgOid.TEXT_ARRAY:
            case OgOid.BPCHAR_ARRAY:
            case OgOid.NUMERIC_ARRAY:
            case OgOid.FLOAT4_ARRAY:
            case OgOid.FLOAT8_ARRAY:
            case OgOid.BOOL_ARRAY:
            case OgOid.DATE_ARRAY:
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
            case OgOid.UUID_ARRAY:
            case OgOid.JSONB_ARRAY:
            case OgOid.JSON_ARRAY:
            case OgOid.TIME_ARRAY:
            case OgOid.TIMETZ_ARRAY:
            case OgOid.TIMESTAMP_ARRAY:
            case OgOid.TIMESTAMPTZ_ARRAY:
            case OgOid.OID_ARRAY:
                return createArrayConverter(column, fieldDefn);

            // TODO DBZ-459 implement support for these array types; for now we just fall back to the default, i.e.
            // having no converter, so to be consistent with the schema definitions above
            case OgOid.BYTEA_ARRAY:
            case OgOid.MONEY_ARRAY:
            case OgOid.NAME_ARRAY:
            case OgOid.INTERVAL_ARRAY:
            case OgOid.VARBIT_ARRAY:
            case OgOid.XML_ARRAY:
            case OgOid.POINT_ARRAY:
            case OgOid.REF_CURSOR_ARRAY:
                return super.converter(column, fieldDefn);

            default:
                if (oidValue == typeRegistry.geometryOid()) {
                    return data -> convertGeometry(column, fieldDefn, data);
                }
                else if (oidValue == typeRegistry.geographyOid()) {
                    return data -> convertGeography(column, fieldDefn, data);
                }
                else if (oidValue == typeRegistry.citextOid()) {
                    return data -> convertCitext(column, fieldDefn, data);
                }
                else if (oidValue == typeRegistry.hstoreOid()) {
                    return data -> convertHStore(column, fieldDefn, data, hStoreMode);
                }
                else if (oidValue == typeRegistry.ltreeOid()) {
                    return data -> convertLtree(column, fieldDefn, data);
                }
                else if (oidValue == typeRegistry.ltreeArrayOid()) {
                    return data -> convertLtreeArray(column, fieldDefn, data);
                }
                else if (oidValue == typeRegistry.geometryArrayOid() ||
                        oidValue == typeRegistry.geographyArrayOid() ||
                        oidValue == typeRegistry.citextArrayOid() ||
                        oidValue == typeRegistry.hstoreArrayOid()) {
                    return createArrayConverter(column, fieldDefn);
                }

                final OpengaussType resolvedType = typeRegistry.get(oidValue);
                if (resolvedType.isArrayType()) {
                    return createArrayConverter(column, fieldDefn);
                }

                final ValueConverter jdbcConverter = super.converter(column, fieldDefn);
                if (jdbcConverter == null) {
                    return includeUnknownDatatypes ? data -> convertBinary(column, fieldDefn, data, binaryMode) : null;
                }
                else {
                    return jdbcConverter;
                }
        }
    }

    private ValueConverter createArrayConverter(Column column, Field fieldDefn) {
        OpengaussType arrayType = typeRegistry.get(column.nativeType());
        OpengaussType elementType = arrayType.getElementType();
        final String elementTypeName = elementType.getName();
        final String elementColumnName = column.name() + "-element";
        final Column elementColumn = Column.editor()
                .name(elementColumnName)
                .jdbcType(elementType.getJdbcId())
                .nativeType(elementType.getOid())
                .type(elementTypeName)
                .optional(true)
                .scale(column.scale().orElse(null))
                .length(column.length())
                .create();

        Schema elementSchema = schemaBuilder(elementColumn)
                .optional()
                .build();

        final Field elementField = new Field(elementColumnName, 0, elementSchema);
        final ValueConverter elementConverter = converter(elementColumn, elementField);

        return data -> convertArray(column, fieldDefn, elementType, elementConverter, data);
    }

    @Override
    protected Object convertTime(Column column, Field fieldDefn, Object data) {
        if (data instanceof String) {
            data = Strings.asDuration((String) data);
        }

        return super.convertTime(column, fieldDefn, data);
    }

    protected Object convertDecimal(Column column, Field fieldDefn, Object data, DecimalMode mode) {
        SpecialValueDecimal value;
        BigDecimal newDecimal;

        if (data instanceof SpecialValueDecimal) {
            value = (SpecialValueDecimal) data;

            if (!value.getDecimalValue().isPresent()) {
                return SpecialValueDecimal.fromLogical(value, mode, column.name());
            }
        }
        else {
            final Object o = toBigDecimal(column, fieldDefn, data);

            if (o == null || !(o instanceof BigDecimal)) {
                return o;
            }
            value = new SpecialValueDecimal((BigDecimal) o);
        }

        newDecimal = withScaleAdjustedIfNeeded(column, value.getDecimalValue().get());

        if (isVariableScaleDecimal(column) && mode == DecimalMode.PRECISE) {
            newDecimal = newDecimal.stripTrailingZeros();
            if (newDecimal.scale() < 0) {
                newDecimal = newDecimal.setScale(0);
            }

            return VariableScaleDecimal.fromLogical(fieldDefn.schema(), new SpecialValueDecimal(newDecimal));
        }

        return SpecialValueDecimal.fromLogical(new SpecialValueDecimal(newDecimal), mode, column.name());
    }

    protected Object convertHStore(Column column, Field fieldDefn, Object data, HStoreHandlingMode mode) {
        if (mode == HStoreHandlingMode.JSON) {
            return convertHstoreToJsonString(column, fieldDefn, data);
        }
        return convertHstoreToMap(column, fieldDefn, data);
    }

    private Object convertLtree(Column column, Field fieldDefn, Object data) {
        return convertValue(column, fieldDefn, data, "", r -> {
            if (data instanceof byte[]) {
                r.deliver(new String((byte[]) data, databaseCharset));
            }
            if (data instanceof String) {
                r.deliver(data);
            }
            else if (data instanceof PGobject) {
                r.deliver(data.toString());
            }
        });
    }

    private Object convertLtreeArray(Column column, Field fieldDefn, Object data) {
        return convertValue(column, fieldDefn, data, Collections.emptyList(), r -> {
            if (data instanceof byte[]) {
                String s = new String((byte[]) data, databaseCharset);
                // remove '{' and '}'
                s = s.substring(1, s.length() - 1);
                List<String> ltrees = Arrays.asList(s.split(","));
                r.deliver(ltrees);
            }
            else if (data instanceof List) {
                List<Object> list = (List<Object>) data;
                List<String> ltrees = new ArrayList<>(list.size());
                for (Object value : list) {
                    ltrees.add(value.toString());
                }
                r.deliver(ltrees);
            }
            else if (data instanceof PgArray) {
                PgArray pgArray = (PgArray) data;
                try {
                    Object[] array = (Object[]) pgArray.getArray();
                    List<String> ltrees = new ArrayList<>(array.length);
                    for (Object value : array) {
                        ltrees.add(value.toString());
                    }
                    r.deliver(ltrees);
                }
                catch (SQLException e) {
                    logger.error("{}Failed to parse PgArray: {}", ErrorCode.SQL_EXCEPTION, pgArray, e);
                }
            }
        });
    }

    private Object convertHstoreToMap(Column column, Field fieldDefn, Object data) {
        return convertValue(column, fieldDefn, data, Collections.emptyMap(), (r) -> {
            if (data instanceof String) {
                r.deliver(HStoreConverter.fromString((String) data));
            }
            else if (data instanceof byte[]) {
                r.deliver(HStoreConverter.fromString(asHstoreString((byte[]) data)));
            }
            else if (data instanceof PGobject) {
                r.deliver(HStoreConverter.fromString(data.toString()));
            }
        });
    }

    /**
     * Returns an Hstore field as string in the form of {@code "key 1"=>"value1", "key_2"=>"val 1"}; i.e. the given byte
     * array is NOT the byte representation returned by {@link HStoreConverter#toBytes(Map,
     * org.postgresql.core.Encoding))}, but the String based representation
     */
    private String asHstoreString(byte[] data) {
        return new String(data, databaseCharset);
    }

    private Object convertHstoreToJsonString(Column column, Field fieldDefn, Object data) {
        return convertValue(column, fieldDefn, data, "{}", (r) -> {
            logger.trace("in ANON: value from data object: *** {} ***", data);
            logger.trace("in ANON: object type is: *** {} ***", data.getClass());
            if (data instanceof String) {
                r.deliver(changePlainStringRepresentationToJsonStringRepresentation(((String) data)));
            }
            else if (data instanceof byte[]) {
                r.deliver(changePlainStringRepresentationToJsonStringRepresentation(asHstoreString((byte[]) data)));
            }
            else if (data instanceof PGobject) {
                r.deliver(changePlainStringRepresentationToJsonStringRepresentation(data.toString()));
            }
            else if (data instanceof java.util.HashMap) {
                r.deliver(convertMapToJsonStringRepresentation((Map<String, String>) data));
            }
        });
    }

    private String changePlainStringRepresentationToJsonStringRepresentation(String text) {
        logger.trace("text value is: {}", text);
        try {
            Map<String, String> map = HStoreConverter.fromString(text);
            return convertMapToJsonStringRepresentation(map);
        }
        catch (Exception e) {
            throw new RuntimeException("Couldn't serialize hstore value into JSON: " + text, e);
        }
    }

    private String convertMapToJsonStringRepresentation(Map<String, String> map) {
        StringWriter writer = new StringWriter();
        try (JsonGenerator jsonGenerator = jsonFactory.createGenerator(writer)) {
            jsonGenerator.writeStartObject();
            for (Entry<String, String> hstoreEntry : map.entrySet()) {
                jsonGenerator.writeStringField(hstoreEntry.getKey(), hstoreEntry.getValue());
            }
            jsonGenerator.writeEndObject();
            jsonGenerator.flush();
            return writer.getBuffer().toString();
        }
        catch (Exception e) {
            throw new RuntimeException("Couldn't serialize hstore value into JSON: " + map, e);
        }
    }

    @Override
    protected Object convertBit(Column column, Field fieldDefn, Object data) {
        if (data instanceof String) {
            data = Integer.valueOf((String) data, 2);
        }
        return super.convertBit(column, fieldDefn, data);
    }

    @Override
    protected Object convertBits(Column column, Field fieldDefn, Object data, int numBytes) {
        if (data instanceof PGobject) {
            // returned by the JDBC driver
            data = ((PGobject) data).getValue();
        }
        if (data instanceof String) {
            String dataStr = (String) data;
            BitSet bitset = new BitSet(dataStr.length());
            int len = dataStr.length();
            for (int i = len - 1; i >= 0; i--) {
                if (dataStr.charAt(i) == '1') {
                    bitset.set(len - i - 1);
                }
            }
            /*
             * Instead of passing the default field precision/length value (= number of bits) divided by 8 = number of bytes
             * see {@link JdbcValueConverters#convertBits}, we re-calculate the length / number of bytes based on the actual
             * content. For example if we have only 3 bits set (b'101') but the field type is BIT VARYING (33) instead of
             * sending 5 bytes (b'10100000' plus 25 zero bits/4 zero bytes) we only pass one little-endian padded byte
             * (b'10100000').
             */
            int numBits = bitset.length();
            int bitBytes = numBits / Byte.SIZE + (numBits % Byte.SIZE == 0 ? 0 : 1);
            return super.convertBits(column, fieldDefn, bitset, bitBytes);
        }
        return super.convertBits(column, fieldDefn, data, numBytes);
    }

    protected Object convertMoney(Column column, Field fieldDefn, Object data, DecimalMode mode) {
        return convertValue(column, fieldDefn, data, BigDecimal.ZERO.setScale(moneyFractionDigits), (r) -> {
            switch (mode) {
                case DOUBLE:
                    if (data instanceof Double) {
                        r.deliver(data);
                    }
                    else if (data instanceof Number) {
                        r.deliver(((Number) data).doubleValue());
                    }
                    break;
                case PRECISE:
                    if (data instanceof Double) {
                        r.deliver(BigDecimal.valueOf((Double) data).setScale(moneyFractionDigits, RoundingMode.HALF_UP));
                    }
                    else if (data instanceof Number) {
                        // the plugin will return a 64bit signed integer where the last #moneyFractionDigits are always decimals
                        r.deliver(BigDecimal.valueOf(((Number) data).longValue()).setScale(moneyFractionDigits, RoundingMode.HALF_UP));
                    }
                    break;
                case STRING:
                    r.deliver(String.valueOf(data));
                    break;
                default:
                    throw new IllegalArgumentException("Unknown decimalMode");
            }
        });
    }

    protected Object convertInterval(Column column, Field fieldDefn, Object data) {
        return convertValue(column, fieldDefn, data, NumberConversions.LONG_FALSE, (r) -> {
            if (data instanceof Number) {
                final long micros = ((Number) data).longValue();
                if (intervalMode == IntervalHandlingMode.STRING) {
                    r.deliver(Interval.toIsoString(0, 0, 0, 0, 0, new BigDecimal(micros).divide(MICROSECONDS_PER_SECOND)));
                }
                else {
                    r.deliver(micros);
                }
            }
            if (data instanceof PGInterval) {
                final PGInterval interval = (PGInterval) data;
                if (intervalMode == IntervalHandlingMode.STRING) {
                    r.deliver(
                            Interval.toIsoString(
                                    interval.getYears(),
                                    interval.getMonths(),
                                    interval.getDays(),
                                    interval.getHours(),
                                    interval.getMinutes(),
                                    new BigDecimal(interval.getSeconds())));
                }
                else {
                    r.deliver(
                            MicroDuration.durationMicros(
                                    interval.getYears(),
                                    interval.getMonths(),
                                    interval.getDays(),
                                    interval.getHours(),
                                    interval.getMinutes(),
                                    interval.getSeconds(),
                                    MicroDuration.DAYS_PER_MONTH_AVG));
                }
            }
        });
    }

    @Override
    protected Object convertTimestampWithZone(Column column, Field fieldDefn, Object data) {
        if (data instanceof java.util.Date) {
            // any Date like subclasses will be given to us by the JDBC driver, which uses the local VM TZ, so we need to go
            // back to GMT
            data = OffsetDateTime.ofInstant(((Date) data).toInstant(), ZoneOffset.UTC);
        }

        if (POSITIVE_INFINITY_OFFSET_DATE_TIME.equals(data)) {
            return "infinity";
        }
        else if (NEGATIVE_INFINITY_OFFSET_DATE_TIME.equals(data)) {
            return "-infinity";
        }

        return super.convertTimestampWithZone(column, fieldDefn, data);
    }

    @Override
    protected Object convertTimeWithZone(Column column, Field fieldDefn, Object data) {
        // during snapshotting; already receiving OffsetTime @ UTC during streaming
        if (data instanceof String) {
            // The TIMETZ column is returned as a String which we initially parse here
            // The parsed offset-time potentially has a zone-offset from the data, shift it after to GMT.
            final OffsetTime offsetTime = OffsetTime.parse((String) data, TIME_WITH_TIMEZONE_FORMATTER);
            data = offsetTime.withOffsetSameInstant(ZoneOffset.UTC);
        }

        return super.convertTimeWithZone(column, fieldDefn, data);
    }

    protected Object convertGeometry(Column column, Field fieldDefn, Object data) {
        final OpengisGeometry empty = OpengisGeometry.createEmpty();
        return convertValue(column, fieldDefn, data, io.debezium.data.geometry.Geometry.createValue(fieldDefn.schema(), empty.getWkb(), empty.getSrid()), (r) -> {
            try {
                final Schema schema = fieldDefn.schema();
                if (data instanceof byte[]) {
                    OpengisGeometry geom = OpengisGeometry.fromHexEwkb(new String((byte[]) data, "ASCII"));
                    r.deliver(io.debezium.data.geometry.Geometry.createValue(schema, geom.getWkb(), geom.getSrid()));
                }
                else if (data instanceof PGobject) {
                    PGobject pgo = (PGobject) data;
                    OpengisGeometry geom = OpengisGeometry.fromHexEwkb(pgo.getValue());
                    r.deliver(io.debezium.data.geometry.Geometry.createValue(schema, geom.getWkb(), geom.getSrid()));
                }
                else if (data instanceof String) {
                    OpengisGeometry geom = OpengisGeometry.fromHexEwkb((String) data);
                    r.deliver(io.debezium.data.geometry.Geometry.createValue(schema, geom.getWkb(), geom.getSrid()));
                }
            }
            catch (IllegalArgumentException | UnsupportedEncodingException e) {
                logger.warn("Error converting to a Geometry type", column);
            }
        });
    }

    protected Object convertGeography(Column column, Field fieldDefn, Object data) {
        final OpengisGeometry empty = OpengisGeometry.createEmpty();
        return convertValue(column, fieldDefn, data, io.debezium.data.geometry.Geography.createValue(fieldDefn.schema(), empty.getWkb(), empty.getSrid()), (r) -> {
            final Schema schema = fieldDefn.schema();
            try {
                if (data instanceof byte[]) {
                    OpengisGeometry geom = OpengisGeometry.fromHexEwkb(new String((byte[]) data, "ASCII"));
                    r.deliver(io.debezium.data.geometry.Geography.createValue(schema, geom.getWkb(), geom.getSrid()));
                }
                else if (data instanceof PGobject) {
                    PGobject pgo = (PGobject) data;
                    OpengisGeometry geom = OpengisGeometry.fromHexEwkb(pgo.getValue());
                    r.deliver(io.debezium.data.geometry.Geography.createValue(schema, geom.getWkb(), geom.getSrid()));
                }
                else if (data instanceof String) {
                    OpengisGeometry geom = OpengisGeometry.fromHexEwkb((String) data);
                    r.deliver(io.debezium.data.geometry.Geography.createValue(schema, geom.getWkb(), geom.getSrid()));
                }
            }
            catch (IllegalArgumentException | UnsupportedEncodingException e) {
                logger.warn("Error converting to a Geography type", column);
            }
        });
    }

    protected Object convertCitext(Column column, Field fieldDefn, Object data) {
        return convertValue(column, fieldDefn, data, "", (r) -> {
            if (data instanceof byte[]) {
                r.deliver(new String((byte[]) data));
            }
            else if (data instanceof String) {
                r.deliver(data);
            }
            else if (data instanceof PGobject) {
                r.deliver(((PGobject) data).getValue());
            }
        });
    }

    /**
     * Converts a value representing a Postgres point for a column, to a Kafka Connect value.
     *
     * @param column the JDBC column; never null
     * @param fieldDefn the Connect field definition for this column; never null
     * @param data a data for the point column, either coming from the JDBC driver or logical decoding plugin
     * @return a value which will be used by Connect to represent the actual point value
     */
    protected Object convertPoint(Column column, Field fieldDefn, Object data) {
        return convertValue(column, fieldDefn, data, Point.createValue(fieldDefn.schema(), 0, 0), (r) -> {
            final Schema schema = fieldDefn.schema();
            if (data instanceof PGpoint) {
                PGpoint pgPoint = (PGpoint) data;
                r.deliver(Point.createValue(schema, pgPoint.x, pgPoint.y));
            }
            else if (data instanceof String) {
                String dataString = data.toString();
                try {
                    PGpoint pgPoint = new PGpoint(dataString);
                    r.deliver(Point.createValue(schema, pgPoint.x, pgPoint.y));
                }
                catch (SQLException e) {
                    logger.warn("Error converting the string '{}' to a PGPoint type for the column '{}'", dataString, column);
                }
            }
            else if (data instanceof PgProto.Point) {
                r.deliver(Point.createValue(schema, ((PgProto.Point) data).getX(), ((PgProto.Point) data).getY()));
            }
        });
    }

    protected Object convertArray(Column column, Field fieldDefn, OpengaussType elementType, ValueConverter elementConverter, Object data) {
        return convertValue(column, fieldDefn, data, Collections.emptyList(), (r) -> {
            if (data instanceof List) {
                r.deliver(((List<?>) data).stream()
                        .map(value -> resolveArrayValue(value, elementType))
                        .map(elementConverter::convert)
                        .collect(Collectors.toList()));
            }
            else if (data instanceof PgArray) {
                try {
                    final Object[] values = (Object[]) ((PgArray) data).getArray();
                    final List<Object> converted = new ArrayList<>(values.length);
                    for (Object value : values) {
                        converted.add(elementConverter.convert(resolveArrayValue(value, elementType)));
                    }
                    r.deliver(converted);
                }
                catch (SQLException e) {
                    throw new ConnectException("Failed to read value of array " + column.name());
                }
            }
        });
    }

    private Object resolveArrayValue(Object value, OpengaussType elementType) {
        // PostgreSQL time data types with time-zones are handled differently when included in an array.
        // The values are automatically translated to the local JVM time-zone and need to be converted back to GMT
        // before delegating the value to the element converter.
        switch (elementType.getOid()) {
            case OgOid.TIMETZ: {
                if (value instanceof java.sql.Time) {
                    ZonedDateTime zonedDateTime = ZonedDateTime.of(LocalDate.now(), ((java.sql.Time) value).toLocalTime(), systemDefault());
                    // Daylight savings gets applied by PgArray, need to account for that here
                    zonedDateTime = zonedDateTime.plus(systemDefault().getRules().getDaylightSavings(zonedDateTime.toInstant()));
                    return OffsetTime.of(zonedDateTime.withZoneSameInstant(ZoneOffset.UTC).toLocalTime(), ZoneOffset.UTC);
                }
                break;
            }
            case OgOid.TIMESTAMPTZ: {
                if (value instanceof java.sql.Timestamp) {
                    // Daylight savings isn't applied here, no need to account for it
                    ZonedDateTime zonedDateTime = ZonedDateTime.of(((java.sql.Timestamp) value).toLocalDateTime(), systemDefault());
                    return OffsetDateTime.of(zonedDateTime.withZoneSameInstant(ZoneOffset.UTC).toLocalDateTime(), ZoneOffset.UTC);
                }
            }
        }

        // no special handling is needed, return value as-is
        return value;
    }

    private boolean isVariableScaleDecimal(final Column column) {
        // TODO: Remove VARIABLE_SCALE_DECIMAL_LENGTH when https://github.com/pgjdbc/pgjdbc/issues/2275
        // is closed.
        return (column.length() == 0 || column.length() == VARIABLE_SCALE_DECIMAL_LENGTH) &&
                column.scale().orElseGet(() -> 0) == 0;
    }

    public static Optional<SpecialValueDecimal> toSpecialValue(String value) {
        switch (value) {
            case N_A_N:
                return Optional.of(SpecialValueDecimal.NOT_A_NUMBER);
            case POSITIVE_INFINITY:
                return Optional.of(SpecialValueDecimal.POSITIVE_INF);
            case NEGATIVE_INFINITY:
                return Optional.of(SpecialValueDecimal.NEGATIVE_INF);
        }

        return Optional.empty();
    }

    protected Object convertTimestampToLocalDateTime(Column column, Field fieldDefn, Object data) {
        if (data == null) {
            return null;
        }
        if (!(data instanceof Timestamp)) {
            return data;
        }
        final Timestamp timestamp = (Timestamp) data;

        if (POSITIVE_INFINITY_TIMESTAMP.equals(timestamp)) {
            return POSITIVE_INFINITY_LOCAL_DATE_TIME;
        }
        else if (NEGATIVE_INFINITY_TIMESTAMP.equals(timestamp)) {
            return NEGATIVE_INFINITY_LOCAL_DATE_TIME;
        }

        final Instant instant = timestamp.toInstant();
        final LocalDateTime utcTime = LocalDateTime
                .ofInstant(instant, ZoneOffset.systemDefault());

        return utcTime;
    }

    @Override
    protected int getTimePrecision(Column column) {
        return column.scale().orElse(-1);
    }

    /**
     * Extracts a value from a PGobject .
     *
     * @param column the column definition describing the {@code data} value; never null
     * @param fieldDefn the field definition; never null
     * @param data the data object to be converted into a Kafka Connect type
     * @return the converted value, or null if the conversion could not be made and the column allows nulls
     * @throws IllegalArgumentException if the value could not be converted but the column does not allow nulls
     */
    @Override
    protected Object convertBinaryToBytes(Column column, Field fieldDefn, Object data) {
        if (data == UnchangedToastedReplicationMessageColumn.UNCHANGED_TOAST_VALUE) {
            return toastPlaceholderBinary;
        }
        if (data instanceof PgArray) {
            data = ((PgArray) data).toString();
        }
        return super.convertBinaryToBytes(column, fieldDefn, (data instanceof PGobject) ? ((PGobject) data).getValue() : data);
    }

    @Override
    protected Object convertBinaryToBase64(Column column, Field fieldDefn, Object data) {
        return super.convertBinaryToBase64(column, fieldDefn, (data instanceof PGobject) ? ((PGobject) data).getValue() : data);
    }

    @Override
    protected Object convertBinaryToHex(Column column, Field fieldDefn, Object data) {
        return super.convertBinaryToHex(column, fieldDefn, (data instanceof PGobject) ? ((PGobject) data).getValue() : data);
    }

    /**
     * Replaces toasted value with a placeholder
     *
     * @param column the column definition describing the {@code data} value; never null
     * @param fieldDefn the field definition; never null
     * @param data the data object to be converted into a Kafka Connect type
     * @return the converted value, or null if the conversion could not be made and the column allows nulls
     * @throws IllegalArgumentException if the value could not be converted but the column does not allow nulls
     */
    @Override
    protected Object convertString(Column column, Field fieldDefn, Object data) {
        if (data == UnchangedToastedReplicationMessageColumn.UNCHANGED_TOAST_VALUE) {
            return toastPlaceholderString;
        }
        return super.convertString(column, fieldDefn, data);
    }

    @Override
    protected Object handleUnknownData(Column column, Field fieldDefn, Object data) {
        if (data == UnchangedToastedReplicationMessageColumn.UNCHANGED_TOAST_VALUE) {
            return toastPlaceholderString;
        }
        return super.handleUnknownData(column, fieldDefn, data);
    }
}
