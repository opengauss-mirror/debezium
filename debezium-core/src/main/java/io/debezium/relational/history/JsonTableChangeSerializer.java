/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational.history;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.DebeziumException;
import io.debezium.document.Array;
import io.debezium.document.Array.Entry;
import io.debezium.document.Document;
import io.debezium.document.Value;
import io.debezium.relational.Column;
import io.debezium.relational.ColumnEditor;
import io.debezium.relational.Table;
import io.debezium.relational.TableEditor;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges.TableChange;
import io.debezium.relational.history.TableChanges.TableChangeType;

/**
 * Ther serializer responsible for converting of {@link TableChanges} into a JSON format.
 *
 * @author Jiri Pechanec
 * <p>
 * Modified by an in 2020.7.2 for constraint feature
 */
public class JsonTableChangeSerializer implements TableChanges.TableChangesSerializer<Array> {

    private static final Logger LOGGER = LoggerFactory.getLogger(JsonTableChangeSerializer.class);
    public static final String TYPE = "type";
    public static final String ID = "id";
    public static final String TABLE = "table";
    public static final String COMMENT = "comment";
    public static final String DEFAULT_CHARSET_NAME = "defaultCharsetName";
    public static final String PRIMARY_KEY_COLUMN_NAMES = "primaryKeyColumnNames";
    public static final String PRIMARY_CONSTRAINT_NAME = "primaryConstraintName";
    public static final String FOREIGN_KEY_COLUMNS = "foreignKeyColumns";
    public static final String INDEXES = "indexes";
    public static final String COLUMNS = "columns";
    public static final String HAS_DEFAULT_VALUE = "hasDefaultValue";
    public static final String DEFAULT_VALUE_EXPRESSION = "defaultValueExpression";
    public static final String ENUM_VALUES = "enumValues";
    public static final String TYPE_NAME = "typeName";
    public static final String LENGTH = "length";
    public static final String SCALE = "scale";
    public static final String TYPE_EXPRESSION = "typeExpression";
    public static final String CHARSET_NAME = "charsetName";
    public static final String NAME = "name";
    public static final String JDBC_TYPE = "jdbcType";
    public static final String NATIVE_TYPE = "nativeType";
    public static final String MODIFY_KEYS = "modifyKeys";
    public static final String POSITION = "position";
    public static final String OPTIONAL = "optional";
    public static final String AUTO_INCREMENTED = "autoIncremented";
    public static final String GENERATED = "generated";

    @Override
    public Array serialize(TableChanges tableChanges) {
        List<Value> values = StreamSupport.stream(tableChanges.spliterator(), false)
                .map(this::toDocument)
                .map(Value::create)
                .collect(Collectors.toList());

        return Array.create(values);
    }

    public Document toDocument(TableChange tableChange) {
        Document document = Document.create();

        document.setString(TYPE, tableChange.getType().name());
        document.setString(ID, tableChange.getId().toDoubleQuotedString());
        document.setDocument(TABLE, toDocument(tableChange.getTable()));
        document.setString(COMMENT, tableChange.getTable().comment());
        return document;
    }

    private Document toDocument(Table table) {
        Document document = Document.create();

        document.set(DEFAULT_CHARSET_NAME, table.defaultCharsetName());
        document.set(PRIMARY_KEY_COLUMN_NAMES, Array.create(table.primaryKeyColumnNames()));
        document.set(PRIMARY_CONSTRAINT_NAME, Array.create(table.primaryConstraintName()));

        if (table.foreignKeyColumns() != null && table.foreignKeyColumns().size() > 0) {
            try {
                document.set(FOREIGN_KEY_COLUMNS, new ObjectMapper().writeValueAsString(table.foreignKeyColumns()));
            }
            catch (JsonProcessingException e) {
                throw new DebeziumException("Document failed to set foreign key, JSON conversion exception.", e);
            }
        }
        if (table.indexes() != null && table.indexes().size() > 0) {
            document.set(INDEXES, Array.create(table.indexes()));
        }

        List<Document> columns = table.columns().stream().map(this::toDocument).collect(Collectors.toList());

        document.setArray(COLUMNS, Array.create(columns));

        return document;
    }

    private Document toDocument(Column column) {
        Document document = Document.create();

        document.setString(NAME, column.name());
        document.setNumber(JDBC_TYPE, column.jdbcType());

        if (column.nativeType() != Column.UNSET_INT_VALUE) {
            document.setNumber(NATIVE_TYPE, column.nativeType());
        }

        document.setString(TYPE_NAME, column.typeName());
        document.setString(TYPE_EXPRESSION, column.typeExpression());
        document.setString(CHARSET_NAME, column.charsetName());

        if (column.length() != Column.UNSET_INT_VALUE) {
            document.setNumber(LENGTH, column.length());
        }

        column.scale().ifPresent(s -> document.setNumber(SCALE, s));

        document.setNumber(POSITION, column.position());
        document.setBoolean(OPTIONAL, column.isOptional());
        document.setBoolean(AUTO_INCREMENTED, column.isAutoIncremented());
        document.setBoolean(GENERATED, column.isGenerated());
        document.setString(COMMENT, column.comment());
        document.setBoolean(HAS_DEFAULT_VALUE, column.hasDefaultValue());

        column.defaultValueExpression().ifPresent(d -> document.setString(DEFAULT_VALUE_EXPRESSION, d));

        Optional.ofNullable(column.enumValues()).map(List::toArray).ifPresent(enums -> document.setArray(ENUM_VALUES, enums));

        Optional.ofNullable(column.modifyKeys()).map(List::toArray).ifPresent(modify -> document.setArray(MODIFY_KEYS, modify));

        return document;
    }

    @Override
    public TableChanges deserialize(Array array, boolean useCatalogBeforeSchema) {
        TableChanges tableChanges = new TableChanges();

        for (Entry entry : array) {
            TableChange change = fromDocument(entry.getValue().asDocument(), useCatalogBeforeSchema);

            if (change.getType() == TableChangeType.CREATE) {
                tableChanges.create(change.getTable());
            }
            else if (change.getType() == TableChangeType.ALTER) {
                tableChanges.alter(change.getTable());
            }
            else if (change.getType() == TableChangeType.DROP) {
                tableChanges.drop(change.getTable());
            }
        }

        return tableChanges;
    }

    private static Table fromDocument(TableId id, Document document) {
        TableEditor editor = Table.editor().tableId(id).setDefaultCharsetName(document.getString(DEFAULT_CHARSET_NAME));
        if (document.getString(COMMENT) != null) {
            editor.setComment(document.getString(COMMENT));
        }
        if (document.has(INDEXES)) {
            Array indexes = document.getArray(INDEXES);
            if (null != indexes && indexes.size() > 0) {
                Set<String> indexSet = document.getArray(INDEXES).streamValues().map(Value::asString).collect(Collectors.toSet());
                editor.setIndexes(indexSet);
            }
        }
        document.getArray(COLUMNS).streamValues().map(Value::asDocument).map(v -> {
            ColumnEditor columnEditor = Column.editor().name(v.getString(NAME)).jdbcType(v.getInteger(JDBC_TYPE));

            Integer nativeType = v.getInteger(NATIVE_TYPE);
            if (nativeType != null) {
                columnEditor.nativeType(nativeType);
            }

            columnEditor.type(v.getString(TYPE_NAME), v.getString(TYPE_EXPRESSION)).charsetName(v.getString(CHARSET_NAME));

            Integer length = v.getInteger(LENGTH);
            if (length != null) {
                columnEditor.length(length);
            }

            Integer scale = v.getInteger(SCALE);
            if (scale != null) {
                columnEditor.scale(scale);
            }

            String columnComment = v.getString(COMMENT);
            if (columnComment != null) {
                columnEditor.comment(columnComment);
            }

            Boolean hasDefaultValue = v.getBoolean(HAS_DEFAULT_VALUE);
            String defaultValueExpression = v.getString(DEFAULT_VALUE_EXPRESSION);
            if (defaultValueExpression != null) {
                columnEditor.defaultValueExpression(defaultValueExpression);
            }
            else if (Boolean.TRUE.equals(hasDefaultValue)) {
                columnEditor.defaultValueExpression(null);
            }

            Array enumValues = v.getArray(ENUM_VALUES);
            if (enumValues != null && !enumValues.isEmpty()) {
                List<String> enumValueList = enumValues.streamValues().map(Value::asString).collect(Collectors.toList());
                columnEditor.enumValues(enumValueList);
            }

            Array modifyKeys = v.getArray(MODIFY_KEYS);
            if (modifyKeys != null && !modifyKeys.isEmpty()) {
                List<String> modifyKeyList = modifyKeys.streamValues().map(Value::asString).collect(Collectors.toList());
                columnEditor.enumValues(modifyKeyList);
            }

            columnEditor.position(v.getInteger(POSITION))
                    .optional(v.getBoolean(OPTIONAL))
                    .autoIncremented(v.getBoolean(AUTO_INCREMENTED))
                    .generated(v.getBoolean(GENERATED));

            return columnEditor.create();
        }).forEach(editor::addColumn);

        editor.setPrimaryKeyNames(document.getArray(PRIMARY_KEY_COLUMN_NAMES).streamValues().map(Value::asString).collect(Collectors.toList()));
        if (document.has(PRIMARY_CONSTRAINT_NAME)) {
            editor.setPrimaryConstraintName(document.getArray(PRIMARY_CONSTRAINT_NAME)
                    .streamValues()
                    .map(Value::asString)
                    .collect(Collectors.toList()));
        }
        String foreignKeyColumns = document.getString(FOREIGN_KEY_COLUMNS);
        if (foreignKeyColumns != null && foreignKeyColumns.length() > 0) {
            try {
                editor.setForeignKeys(new ObjectMapper().readValue(foreignKeyColumns, List.class));
            }
            catch (JsonMappingException e) {
                throw new DebeziumException("Document failed to get foreign key, JsonMapping exception.", e);
            }
            catch (JsonProcessingException e) {
                throw new DebeziumException("Document failed to get foreign key, JSON Processing exception.", e);
            }
        }

        return editor.create();
    }

    public static TableChange fromDocument(Document document, boolean useCatalogBeforeSchema) {
        TableChangeType type = TableChangeType.valueOf(document.getString(TYPE));
        TableId id = TableId.parse(document.getString(ID), useCatalogBeforeSchema);
        Table table = null;

        if (type == TableChangeType.CREATE || type == TableChangeType.ALTER) {
            table = fromDocument(id, document.getDocument(TABLE));
        }
        else {
            table = Table.editor().tableId(id).create();
        }
        return new TableChange(type, table);
    }
}
