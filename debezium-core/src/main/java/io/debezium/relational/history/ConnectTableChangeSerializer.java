/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational.history;

import io.debezium.relational.Column;
import io.debezium.relational.Index;
import io.debezium.relational.Table;
import io.debezium.relational.history.TableChanges.TableChange;
import io.debezium.util.SchemaNameAdjuster;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Ther serializer responsible for converting of {@link TableChanges} into an array of {@link Struct}s.
 *
 * @author Jiri Pechanec
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 * Modified by an in 2020.7.2 for constraint feature
 *
 */
public class ConnectTableChangeSerializer implements TableChanges.TableChangesSerializer<List<Struct>> {

    public static final String ID_KEY = "id";
    public static final String TYPE_KEY = "type";
    public static final String TABLE_KEY = "table";
    public static final String DEFAULT_CHARSET_NAME_KEY = "defaultCharsetName";
    public static final String PRIMARY_KEY_COLUMN_NAMES_KEY = "primaryKeyColumnNames";

    public static final String PRIMARY_CONSTRAINT_NAME_KEY = "primaryConstraintName";
    public static final String PRIMARY_KEY_COLUMN_CHENGES_KEY = "primaryKeyColumnChanges";
    public static final String FOREIGN_KEY_COLUMN_KEY = "foreignKeyColumns";
    public static final String UNIQUE_COLUMN_KEY = "uniqueColumns";
    public static final String CHECK_COLUMN_KEY = "checkColumns";
    public static final String COLUMNS_KEY = "columns";
    public static final String NAME_KEY = "name";
    public static final String JDBC_TYPE_KEY = "jdbcType";
    public static final String NATIVE_TYPE_KEY = "nativeType";
    public static final String TYPE_NAME_KEY = "typeName";
    public static final String TYPE_EXPRESSION_KEY = "typeExpression";
    public static final String CHARSET_NAME_KEY = "charsetName";
    public static final String LENGTH_KEY = "length";
    public static final String SCALE_KEY = "scale";
    public static final String POSITION_KEY = "position";
    public static final String OPTIONAL_KEY = "optional";
    public static final String DEFAULT_VALUE_EXPRESSION_KEY = "defaultValueExpression";
    public static final String AUTO_INCREMENTED_KEY = "autoIncremented";
    public static final String GENERATED_KEY = "generated";
    public static final String COMMENT_KEY = "comment";
    public static final String MODIFY_KEYS_KEY = "modifyKeys";
    public static final String CHANGE_COLUMN = "changeColumns";
    public static final String COLUMN_EXPR = "columnExpr";

    public static final String DESC = "desc";
    public static final String INCLUDE_COLUMN = "includeColumn";
    public static final String INDEX_ID = "indexId";
    public static final String INDEX_NAME = "indexName";
    public static final String TABLE_ID = "tableId";

    public static final String TABLE_NAME = "tableName";

    public static final String SCHEMA_NAME = "schemaName";
    public static final String INDEX_COLUMN_EXPR_KEY = "indexColumnExpr";
    public static final String INDEX_CHANGES = "indexChanges";
    public static final String INDEXES_KEY = "indexes";

    public static final String INDEX_UNIQUE_KEY = "unique";

    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectTableChangeSerializer.class);
    private static final SchemaNameAdjuster schemaNameAdjuster = SchemaNameAdjuster.create();

    private static final Schema COLUMN_SCHEMA = SchemaBuilder.struct()
            .name(schemaNameAdjuster.adjust("io.debezium.connector.schema.Column"))
            .field(NAME_KEY, Schema.STRING_SCHEMA)
            .field(JDBC_TYPE_KEY, Schema.INT32_SCHEMA)
            .field(NATIVE_TYPE_KEY, Schema.OPTIONAL_INT32_SCHEMA)
            .field(TYPE_NAME_KEY, Schema.STRING_SCHEMA)
            .field(TYPE_EXPRESSION_KEY, Schema.OPTIONAL_STRING_SCHEMA)
            .field(CHARSET_NAME_KEY, Schema.OPTIONAL_STRING_SCHEMA)
            .field(LENGTH_KEY, Schema.OPTIONAL_INT32_SCHEMA)
            .field(SCALE_KEY, Schema.OPTIONAL_INT32_SCHEMA)
            .field(POSITION_KEY, Schema.INT32_SCHEMA)
            .field(OPTIONAL_KEY, Schema.OPTIONAL_BOOLEAN_SCHEMA)
            .field(DEFAULT_VALUE_EXPRESSION_KEY, Schema.OPTIONAL_STRING_SCHEMA)
            .field(AUTO_INCREMENTED_KEY, Schema.OPTIONAL_BOOLEAN_SCHEMA)
            .field(GENERATED_KEY, Schema.OPTIONAL_BOOLEAN_SCHEMA)
            .field(COMMENT_KEY, Schema.OPTIONAL_STRING_SCHEMA)
            .field(MODIFY_KEYS_KEY, SchemaBuilder.array(Schema.STRING_SCHEMA).optional().build())
            .build();
    private static final Schema INDEX_COLUMN_EXPR = SchemaBuilder.struct()
            .name(schemaNameAdjuster.adjust("io.debezium.connector.schema.IndexColumn"))
            .field(COLUMN_EXPR, Schema.OPTIONAL_STRING_SCHEMA)
            .field(DESC, Schema.OPTIONAL_BOOLEAN_SCHEMA)
            .field(INCLUDE_COLUMN,
                    SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).optional().build())
            .build();

    /**
     * index schema builder
     */
    private static final Schema INDEX_SCHEMA = SchemaBuilder.struct()
            .name(schemaNameAdjuster.adjust("io.debezium.connector.schema.Index"))
            .field(INDEX_ID, Schema.OPTIONAL_STRING_SCHEMA)
            .field(INDEX_NAME, Schema.OPTIONAL_STRING_SCHEMA)
            .field(TABLE_ID, Schema.OPTIONAL_STRING_SCHEMA)
            .field(TABLE_NAME, Schema.OPTIONAL_STRING_SCHEMA)
            .field(SCHEMA_NAME, Schema.OPTIONAL_STRING_SCHEMA)
            .field(INDEX_UNIQUE_KEY, Schema.OPTIONAL_BOOLEAN_SCHEMA)
            .field(INDEX_COLUMN_EXPR_KEY, SchemaBuilder.array(INDEX_COLUMN_EXPR).optional().build())
            .optional()
            .build();

    private static final Schema TABLE_SCHEMA = SchemaBuilder.struct()
            .name(schemaNameAdjuster.adjust("io.debezium.connector.schema.Table"))
            .field(DEFAULT_CHARSET_NAME_KEY, Schema.OPTIONAL_STRING_SCHEMA)
            .field(PRIMARY_KEY_COLUMN_NAMES_KEY, SchemaBuilder.array(Schema.STRING_SCHEMA).optional().build())
            .field(PRIMARY_CONSTRAINT_NAME_KEY, SchemaBuilder.array(Schema.STRING_SCHEMA).optional().build())
            .field(PRIMARY_KEY_COLUMN_CHENGES_KEY, SchemaBuilder
                    .array(SchemaBuilder.map(Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_STRING_SCHEMA).optional().build())
                    .optional().build())
            .field(FOREIGN_KEY_COLUMN_KEY, SchemaBuilder
                    .array(SchemaBuilder.map(Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_STRING_SCHEMA).optional().build())
                    .optional().build())
            .field(UNIQUE_COLUMN_KEY, SchemaBuilder
                    .array(SchemaBuilder.map(Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_STRING_SCHEMA).optional().build())
                    .optional().build())
            .field(CHECK_COLUMN_KEY, SchemaBuilder
                    .array(SchemaBuilder.map(Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_STRING_SCHEMA).optional().build())
                    .optional().build())
            .field(COLUMNS_KEY, SchemaBuilder.array(COLUMN_SCHEMA).build())
            .field(COMMENT_KEY, Schema.OPTIONAL_STRING_SCHEMA)
            .field(CHANGE_COLUMN, SchemaBuilder.map(Schema.STRING_SCHEMA, SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA)).optional().build())
            .field(INDEXES_KEY, SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).optional().build())
            .field(INDEX_CHANGES, INDEX_SCHEMA)
            .build();

    public static final Schema CHANGE_SCHEMA = SchemaBuilder.struct()
            .name(schemaNameAdjuster.adjust("io.debezium.connector.schema.Change"))
            .field(TYPE_KEY, Schema.STRING_SCHEMA)
            .field(ID_KEY, Schema.STRING_SCHEMA)
            .field(TABLE_KEY, TABLE_SCHEMA)
            .build();

    @Override
    public List<Struct> serialize(TableChanges tableChanges) {
        return StreamSupport.stream(tableChanges.spliterator(), false)
                .map(this::toStruct)
                .collect(Collectors.toList());
    }

    public Struct toStruct(TableChange tableChange) {
        final Struct struct = new Struct(CHANGE_SCHEMA);

        struct.put(TYPE_KEY, tableChange.getType().name());
        struct.put(ID_KEY, tableChange.getId().toDoubleQuotedString());
        struct.put(TABLE_KEY, toStruct(tableChange.getTable()));
        return struct;
    }

    private Struct toStruct(Table table) {
        final Struct struct = new Struct(TABLE_SCHEMA);

        struct.put(DEFAULT_CHARSET_NAME_KEY, table.defaultCharsetName());
        struct.put(PRIMARY_KEY_COLUMN_NAMES_KEY, table.primaryKeyColumnNames());
        struct.put(PRIMARY_CONSTRAINT_NAME_KEY, table.primaryConstraintName());
        struct.put(FOREIGN_KEY_COLUMN_KEY, table.foreignKeyColumns());
        struct.put(UNIQUE_COLUMN_KEY, table.uniqueColumns());
        struct.put(CHECK_COLUMN_KEY, table.checkColumns());
        if (!table.changeColumn().isEmpty()) {
            struct.put(CHANGE_COLUMN, table.changeColumn());
        }
        else {
            LOGGER.info("change column is empty");
        }
        if (null != table.indexes()) {
            struct.put(INDEXES_KEY, new ArrayList<>(table.indexes()));
        }

        if (null != table.indexChanges() && table.indexChanges().getIndexName() != null) {
            struct.put(INDEX_CHANGES, toStruct(table.indexChanges()));
        }

        List<Map<String, String>> primaryKeyColumnChanges = table.primaryKeyColumnChanges();
        if (primaryKeyColumnChanges != null && primaryKeyColumnChanges.size() > 0) {
            struct.put(PRIMARY_KEY_COLUMN_CHENGES_KEY, primaryKeyColumnChanges);
        }

        final List<Struct> columns = table.columns().stream()
                .map(this::toStruct)
                .filter(columnStruct -> columnStruct != null)
                .collect(Collectors.toList());

        struct.put(COLUMNS_KEY, columns);
        struct.put(COMMENT_KEY, table.comment());
        return struct;
    }

    private Struct toStruct(Index index) {
        final Struct struct = new Struct(INDEX_SCHEMA);
        struct.put(INDEX_ID, index.getIndexId());
        struct.put(INDEX_NAME, index.getIndexName());
        struct.put(TABLE_ID, index.getTableId());
        struct.put(SCHEMA_NAME, index.getSchemaName());
        struct.put(TABLE_NAME, index.getTableName());
        struct.put(INDEX_UNIQUE_KEY, index.getUnique());
        if (index.getIndexColumnExpr() != null) {
            struct.put(INDEX_COLUMN_EXPR_KEY, index.getIndexColumnExpr().stream().map(this::toStruct).collect(Collectors.toList()));
        }
        return struct;
    }

    private Struct toStruct(Index.IndexColumnExpr indexColumnExpr) {
        final Struct struct = new Struct(INDEX_COLUMN_EXPR);
        struct.put(COLUMN_EXPR, indexColumnExpr.getColumnExpr());
        struct.put(DESC, indexColumnExpr.isDesc());
        struct.put(INCLUDE_COLUMN, indexColumnExpr.getIncludeColumn());
        return struct;
    }

    private Struct toStruct(Column column) {
        final Struct struct = new Struct(COLUMN_SCHEMA);

        struct.put(NAME_KEY, column.name());
        struct.put(JDBC_TYPE_KEY, column.jdbcType());

        if (column.nativeType() != Column.UNSET_INT_VALUE) {
            struct.put(NATIVE_TYPE_KEY, column.nativeType());
        }

        if (column.typeName() == null) {
            return null;
        }

        struct.put(TYPE_NAME_KEY, column.typeName());
        struct.put(TYPE_EXPRESSION_KEY, column.typeExpression());
        struct.put(CHARSET_NAME_KEY, column.charsetName());

        if (column.length() != Column.UNSET_INT_VALUE) {
            struct.put(LENGTH_KEY, column.length());
        }

        column.scale().ifPresent(s -> struct.put(SCALE_KEY, s));

        column.defaultValueExpression().ifPresent(s -> struct.put(DEFAULT_VALUE_EXPRESSION_KEY, s));

        struct.put(POSITION_KEY, column.position());
        struct.put(OPTIONAL_KEY, column.isOptional());
        struct.put(AUTO_INCREMENTED_KEY, column.isAutoIncremented());
        struct.put(GENERATED_KEY, column.isGenerated());
        struct.put(COMMENT_KEY, column.comment());

        List<String> modifyKeys = column.modifyKeys();
        if (modifyKeys != null && modifyKeys.size() > 0) {
            struct.put(MODIFY_KEYS_KEY, modifyKeys);
        }

        return struct;
    }

    @Override
    public TableChanges deserialize(List<Struct> data, boolean useCatalogBeforeSchema) {
        throw new UnsupportedOperationException("Deserialization from Connect Struct is not supported");
    }
}
