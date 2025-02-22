/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.connection.wal2json;

import java.math.BigDecimal;

import io.debezium.connector.postgresql.connection.AbstractColumnValue;
import io.debezium.data.SpecialValueDecimal;
import io.debezium.document.Value;
import io.debezium.util.Strings;

/**
 * @author Chris Cranford
 */
class Wal2JsonColumnValue extends AbstractColumnValue<Value> {
    private static final String NAN = "NaN";
    private static final String POSITIVE_INFINITY = "infinity";
    private static final String NEGATIVE_INFINITY = "-infinity";

    private Value value;

    Wal2JsonColumnValue(Value value) {
        this.value = value;
    }

    @Override
    public Value getRawValue() {
        return value;
    }

    @Override
    public boolean isNull() {
        return value.isNull();
    }

    @Override
    public String asString() {
        return value.asString();
    }

    @Override
    public Boolean asBoolean() {
        if (value.isBoolean()) {
            return value.asBoolean();
        }
        else if (value.isString()) {
            return "t".equalsIgnoreCase(value.asString());
        }
        else {
            return null;
        }
    }

    @Override
    public Integer asInteger() {
        if (value.isNumber()) {
            return value.asInteger();
        }
        else if (value.isString()) {
            return Integer.valueOf(value.asString());
        }
        else {
            return null;
        }
    }

    @Override
    public Long asLong() {
        if (value.isNumber()) {
            return value.asLong();
        }
        else if (value.isString()) {
            return Long.valueOf(value.asString());
        }
        else {
            return null;
        }
    }

    @Override
    public Float asFloat() {
        return value.isNumber() ? value.asFloat() : Float.valueOf(value.asString());
    }

    @Override
    public Double asDouble() {
        return value.isNumber() ? value.asDouble() : Double.valueOf(value.asString());
    }

    @Override
    public SpecialValueDecimal asDecimal() {
        if (value.isInteger()) {
            return new SpecialValueDecimal(new BigDecimal(value.asInteger()));
        }
        else if (value.isLong()) {
            return new SpecialValueDecimal(new BigDecimal(value.asLong()));
        }
        else if (value.isBigInteger()) {
            return new SpecialValueDecimal(new BigDecimal(value.asBigInteger()));
        }
        String valueStr = value.asString();
        if (valueStr.equalsIgnoreCase(NAN)) {
            return SpecialValueDecimal.NOT_A_NUMBER;
        }
        if (valueStr.equalsIgnoreCase(POSITIVE_INFINITY)) {
            return SpecialValueDecimal.POSITIVE_INF;
        }
        if (valueStr.equalsIgnoreCase(NEGATIVE_INFINITY)) {
            return SpecialValueDecimal.NEGATIVE_INF;
        }
        return SpecialValueDecimal.valueOf(valueStr);
    }

    @Override
    public byte[] asByteArray() {
        return Strings.hexStringToByteArray(value.asString());
    }
}
