/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Modified by an in 2020.7.2 for constraint feature
 */
final class NoOpTableEditorImpl implements TableEditor {

    private TableId id;
    private boolean uniqueValues = false;
    private String defaultCharsetName;
    private String comment;

    protected NoOpTableEditorImpl() {
    }

    @Override
    public TableId tableId() {
        return id;
    }

    @Override
    public TableEditor tableId(TableId id) {
        this.id = id;
        return this;
    }

    @Override
    public List<Column> columns() {
        return Collections.emptyList();
    }

    @Override
    public Column columnWithName(String name) {
        return null;
    }

    protected boolean hasColumnWithName(String name) {
        return false;
    }

    @Override
    public List<String> primaryKeyColumnNames() {
        return Collections.emptyList();
    }

    @Override
    public Map<String, List<String>> getChangeColumn() {
        return null;
    }

    @Override
    public TableEditor addColumns(Column... columns) {
        return this;
    }

    @Override
    public TableEditor addColumns(Iterable<Column> columns) {
        return this;
    }

    @Override
    public TableEditor setColumns(Column... columns) {
        return this;
    }

    @Override
    public TableEditor setColumns(Iterable<Column> columns) {
        return this;
    }

    @Override
    public TableEditor setPrimaryKeyNames(String... pkColumnNames) {
        return this;
    }

    @Override
    public TableEditor setPrimaryKeyNames(List<String> pkColumnNames) {
        return this;
    }

    @Override
    public TableEditor setUniqueValues() {
        this.uniqueValues = true;
        return this;
    }

    @Override
    public boolean hasUniqueValues() {
        return uniqueValues;
    }

    @Override
    public TableEditor setDefaultCharsetName(String charsetName) {
        this.defaultCharsetName = charsetName;
        return this;
    }

    @Override
    public TableEditor setComment(String comment) {
        this.comment = comment;
        return this;
    }

    @Override
    public TableEditor setChangeColumn(Map<String, List<String>> changeColumn) {
        return this;
    }

    @Override
    public boolean hasDefaultCharsetName() {
        return this.defaultCharsetName != null && !this.defaultCharsetName.trim().isEmpty();
    }

    @Override
    public boolean hasComment() {
        return this.comment != null && !this.comment.trim().isEmpty();
    }

    @Override
    public TableEditor removeColumn(String columnName) {
        return this;
    }

    @Override
    public TableEditor updateColumn(Column column) {
        return this;
    }

    @Override
    public TableEditor reorderColumn(String columnName, String afterColumnName) {
        return this;
    }

    @Override
    public TableEditor renameColumn(String existingName, String newName) {
        return this;
    }

    @Override
    public String toString() {
        return create().toString();
    }

    @Override
    public Table create() {
        if (id == null) {
            throw new IllegalStateException("Unable to create a table from an editor that has no table ID");
        }
        List<Column> columns = new ArrayList<>();
        return new TableImpl(id,
                columns,
                primaryKeyColumnNames(),
                primaryConstraintName(),
                primaryKeyColumnChanges(),
                constraintChanges(),
                foreignKeyColumns(),
                uniqueColumns(),
                checkColumns(),
                getChangeColumn(),
                getChangeIndex(),
                getIndexes(),
                defaultCharsetName,
                comment);
    }

    @Override
    public List<Map<String, String>> foreignKeyColumns() {
        return Collections.emptyList();
    }

    @Override
    public TableEditor setForeignKeys(List<Map<String, String>> fkColumns) {
        return this;
    }

    @Override
    public List<Map<String, String>> uniqueColumns() {
        return Collections.emptyList();
    }

    @Override
    public List<Map<String, String>> checkColumns() {
        return Collections.emptyList();
    }

    @Override
    public TableEditor setUniqueColumns(List<Map<String, String>> uniqueColumns) {
        return this;
    }

    @Override
    public TableEditor setCheckColumns(List<Map<String, String>> checkColumns) {
        return this;
    }

    @Override
    public List<Map<String, String>> primaryKeyColumnChanges() {
        return Collections.emptyList();
    }

    @Override
    public TableEditor setPrimaryKeyChanges(List<Map<String, String>> pkColumnChanges) {
        return this;
    }

    @Override
    public boolean clearConstraint() {
        return false;
    }

    @Override
    public List<Map<String, String>> constraintChanges() {
        return Collections.emptyList();
    }

    @Override
    public TableEditor setConstraintChanges(List<Map<String, String>> constraintChanges) {
        return this;
    }
}
