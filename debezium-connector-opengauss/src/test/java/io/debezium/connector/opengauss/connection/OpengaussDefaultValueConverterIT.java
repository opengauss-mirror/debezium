/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.opengauss.connection;

import java.nio.charset.Charset;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Optional;

import io.debezium.connector.opengauss.OpengaussConnectorConfig;
import io.debezium.connector.opengauss.OpengaussValueConverter;
import io.debezium.connector.opengauss.TestHelper;
import io.debezium.connector.opengauss.TypeRegistry;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.doc.FixFor;
import io.debezium.relational.Column;
import io.debezium.relational.RelationalDatabaseConnectorConfig.DecimalHandlingMode;

public class OpengaussDefaultValueConverterIT {

    private OpengaussConnection postgresConnection;
    private OpengaussValueConverter postgresValueConverter;
    private OpengaussDefaultValueConverter postgresDefaultValueConverter;

    @Before
    public void before() throws SQLException {
        TestHelper.dropAllSchemas();

        postgresConnection = TestHelper.create();

        OpengaussConnectorConfig postgresConnectorConfig = new OpengaussConnectorConfig(TestHelper.defaultJdbcConfig());
        postgresValueConverter = OpengaussValueConverter.of(
                postgresConnectorConfig,
                Charset.defaultCharset(),
                new TypeRegistry(postgresConnection));

        postgresDefaultValueConverter = new OpengaussDefaultValueConverter(
                postgresValueConverter, postgresConnection.getTimestampUtils());
    }

    @After
    public void closeConnection() {
        if (postgresConnection != null) {
            postgresConnection.close();
        }
    }

    @Test
    @FixFor("DBZ-4137")
    public void shouldReturnNullForNumericDefaultValue() {
        final Column NumericalColumn = Column.editor().type("numeric", "numeric(19, 4)")
                .jdbcType(Types.NUMERIC).defaultValueExpression("NULL::numeric").optional(true).create();
        final Optional<Object> numericalConvertedValue = postgresDefaultValueConverter.parseDefaultValue(
                NumericalColumn,
                NumericalColumn.defaultValueExpression().orElse(null));

        Assert.assertEquals(numericalConvertedValue, Optional.empty());
    }

    @Test
    @FixFor("DBZ-4137")
    public void shouldReturnNullForNumericDefaultValueUsingDecimalHandlingModePrecise() {
        Configuration config = TestHelper.defaultJdbcConfig()
                .edit()
                .with(OpengaussConnectorConfig.DECIMAL_HANDLING_MODE, DecimalHandlingMode.PRECISE)
                .build();

        OpengaussConnectorConfig postgresConnectorConfig = new OpengaussConnectorConfig(config);
        OpengaussValueConverter postgresValueConverter = OpengaussValueConverter.of(
                postgresConnectorConfig,
                Charset.defaultCharset(),
                new TypeRegistry(postgresConnection));

        OpengaussDefaultValueConverter postgresDefaultValueConverter = new OpengaussDefaultValueConverter(
                postgresValueConverter, postgresConnection.getTimestampUtils());

        final Column NumericalColumn = Column.editor().type("numeric", "numeric(19, 4)")
                .jdbcType(Types.NUMERIC).defaultValueExpression("NULL::numeric").optional(true).create();
        final Optional<Object> numericalConvertedValue = postgresDefaultValueConverter.parseDefaultValue(
                NumericalColumn,
                NumericalColumn.defaultValueExpression().orElse(null));

        Assert.assertEquals(numericalConvertedValue, Optional.empty());
    }

    @Test
    @FixFor("DBZ-3989")
    public void shouldTrimNumericalDefaultValueAndShouldNotTrimNonNumericalDefaultValue() {
        final Column NumericalColumn = Column.editor().type("int8").jdbcType(Types.INTEGER).defaultValueExpression(" 1 ").create();
        final Optional<Object> numericalConvertedValue = postgresDefaultValueConverter.parseDefaultValue(
                NumericalColumn,
                NumericalColumn.defaultValueExpression().orElse(null));

        Assert.assertEquals(numericalConvertedValue, Optional.of(1));

        final Column nonNumericalColumn = Column.editor().type("text").jdbcType(Types.VARCHAR).defaultValueExpression(" 1 ").create();
        final Optional<Object> nonNumericalConvertedValue = postgresDefaultValueConverter.parseDefaultValue(
                nonNumericalColumn,
                NumericalColumn.defaultValueExpression().orElse(null));

        Assert.assertEquals(nonNumericalConvertedValue, Optional.of(" 1 "));
    }

}
