/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.opengauss.connection.ogoutput;

import java.math.BigDecimal;
import java.math.BigInteger;

import io.debezium.connector.opengauss.connection.AbstractColumnValue;
import io.debezium.data.SpecialValueDecimal;
import io.debezium.util.Strings;

/**
 * @author Chris Cranford
 */
class OgOutputColumnValue extends AbstractColumnValue<String> {

    private String value;

    OgOutputColumnValue(String value) {
        this.value = value;
    }

    @Override
    public String getRawValue() {
        return value;
    }

    @Override
    public boolean isNull() {
        return value == null;
    }

    @Override
    public String asString() {
        return value;
    }

    @Override
    public Boolean asBoolean() {
        return "t".equalsIgnoreCase(value);
    }

    @Override
    public Integer asInteger() {
        return Integer.valueOf(value);
    }

    @Override
    public Long asLong() {
        return Long.valueOf(value);
    }

    @Override
    public Float asFloat() {
        return Float.valueOf(value);
    }

    @Override
    public Double asDouble() {
        return Double.valueOf(value);
    }

    public String asNumberString(){
        BigInteger number = new BigInteger(value, 16);
        return number.toString();
    }

    @Override
    public SpecialValueDecimal asDecimal() {
        if ("NaN".equals(value)) {
            return SpecialValueDecimal.NOT_A_NUMBER;
        }
        else {
            return new SpecialValueDecimal(new BigDecimal(value));
        }
    }

    @Override
    public byte[] asByteArray() {
        return Strings.hexStringToByteArray(value.substring(2));
    }

    @Override
    public String toString() {
        return "OgOutputColumnValue{" +
                "value='" + value + '\'' +
                '}';
    }
}
