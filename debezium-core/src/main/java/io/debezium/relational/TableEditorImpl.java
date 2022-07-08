/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Modified by an in 2020.7.2 for constraint feature
 */
class TableEditorImpl implements TableEditor {

    private TableId id;
    private LinkedHashMap<String, Column> sortedColumns = new LinkedHashMap<>();
    private final List<String> pkColumnNames = new ArrayList<>();
    private final List<Map<String, String>> pkColumnChanges = new ArrayList<>();
    private final List<Map<String, String>> constraintChanges = new ArrayList<>();
    private final List<Map<String, String>> fkColumns = new ArrayList<>();
    private final List<Map<String, String>> uniqueColumns = new ArrayList<>();
    private final List<Map<String, String>> checkColumns = new ArrayList<>();
    private boolean uniqueValues = false;
    private String defaultCharsetName;
    private String comment;

    protected TableEditorImpl() {
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
        return Collections.unmodifiableList(new ArrayList<>(sortedColumns.values()));
    }

    @Override
    public Column columnWithName(String name) {
        return sortedColumns.get(name.toLowerCase());
    }

    protected boolean hasColumnWithName(String name) {
        return columnWithName(name) != null;
    }

    @Override
    public List<String> primaryKeyColumnNames() {
        return uniqueValues ? columnNames() : Collections.unmodifiableList(pkColumnNames);
    }

    @Override
    public List<Map<String, String>> constraintChanges() {
        return constraintChanges;
    }

    @Override
    public List<Map<String, String>> primaryKeyColumnChanges() {
        return Collections.unmodifiableList(pkColumnChanges);
    }

    @Override
    public List<Map<String, String>> foreignKeyColumns() {
        return Collections.unmodifiableList(fkColumns);
    }

    @Override
    public List<Map<String, String>> uniqueColumns() {
        return Collections.unmodifiableList(uniqueColumns);
    }

    @Override
    public List<Map<String, String>> checkColumns() {
        return Collections.unmodifiableList(checkColumns);
    }

    @Override
    public TableEditor addColumns(Column... columns) {
        for (Column column : columns) {
            add(column);
        }
        assert positionsAreValid();
        return this;
    }

    @Override
    public TableEditor addColumns(Iterable<Column> columns) {
        columns.forEach(this::add);
        assert positionsAreValid();
        return this;
    }

    protected void add(Column defn) {
        if (defn != null) {
            Column existing = columnWithName(defn.name());
            int position = existing != null ? existing.position() : sortedColumns.size() + 1;
            sortedColumns.put(defn.name().toLowerCase(), defn.edit().position(position).create());
        }
        assert positionsAreValid();
    }

    @Override
    public TableEditor setColumns(Column... columns) {
        sortedColumns.clear();
        addColumns(columns);
        assert positionsAreValid();
        return this;
    }

    @Override
    public TableEditor setColumns(Iterable<Column> columns) {
        sortedColumns.clear();
        addColumns(columns);
        assert positionsAreValid();
        return this;
    }

    protected void updatePrimaryKeys() {
        if (!uniqueValues) {
            // table does have any primary key --> we need to remove it
            this.pkColumnNames.removeIf(pkColumnName -> {
                final boolean pkColumnDoesNotExists = !hasColumnWithName(pkColumnName);
                if (pkColumnDoesNotExists) {
                    throw new IllegalArgumentException(
                            "The column \"" + pkColumnName + "\" is referenced as PRIMARY KEY, but a matching column is not defined in table \"" + tableId() + "\"!");
                }
                return pkColumnDoesNotExists;
            });
        }
    }

    @Override
    public TableEditor setPrimaryKeyNames(String... pkColumnNames) {
        return setPrimaryKeyNames(Arrays.asList(pkColumnNames));
    }

    @Override
    public TableEditor setPrimaryKeyChanges(List<Map<String, String>> pkColumnChanges) {
        this.pkColumnChanges.clear();
        this.pkColumnChanges.addAll(pkColumnChanges);
        return this;
    }

    @Override
    public TableEditor setConstraintChanges(List<Map<String, String>> constraintChanges) {
        this.constraintChanges.addAll(constraintChanges);
        return this;
    }

    @Override
    public TableEditor setForeignKeys(List<Map<String, String>> fkColumns) {
        this.fkColumns.addAll(fkColumns);
        return this;
    }

    @Override
    public TableEditor setUniqueColumns(List<Map<String, String>> uniqueColumns) {
        this.uniqueColumns.addAll(uniqueColumns);
        return this;
    }

    @Override
    public TableEditor setCheckColumns(List<Map<String, String>> checkColumns) {
        this.checkColumns.addAll(checkColumns);
        return this;
    }

    @Override
    public TableEditor setPrimaryKeyNames(List<String> pkColumnNames) {
        this.pkColumnNames.clear();
        this.pkColumnNames.addAll(pkColumnNames);
        uniqueValues = false;
        return this;
    }

    @Override
    public TableEditor setUniqueValues() {
        pkColumnNames.clear();
        uniqueValues = true;
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
    public boolean hasDefaultCharsetName() {
        return this.defaultCharsetName != null && !this.defaultCharsetName.trim().isEmpty();
    }

    @Override
    public boolean hasComment() {
        return this.comment != null && !this.comment.trim().isEmpty();
    }

    @Override
    public TableEditor removeColumn(String columnName) {
        Column existing = sortedColumns.remove(columnName.toLowerCase());
        if (existing != null) {
            updatePositions();
        }
        assert positionsAreValid();
        pkColumnNames.remove(columnName);
        return this;
    }

    @Override
    public TableEditor updateColumn(Column newColumn) {
        setColumns(columns().stream()
                .map(c -> c.name().equals(newColumn.name()) ? newColumn : c)
                .collect(Collectors.toList()));
        return this;
    }

    @Override
    public TableEditor reorderColumn(String columnName, String afterColumnName) {
        Column columnToMove = columnWithName(columnName);
        if (columnToMove == null) {
            throw new IllegalArgumentException("No column with name '" + columnName + "'");
        }
        Column afterColumn = afterColumnName == null ? null : columnWithName(afterColumnName);
        if (afterColumn != null && afterColumn.position() == sortedColumns.size()) {
            // Just append ...
            sortedColumns.remove(columnName);
            sortedColumns.put(columnName, columnToMove);
        }
        else {
            LinkedHashMap<String, Column> newColumns = new LinkedHashMap<>();
            sortedColumns.remove(columnName.toLowerCase());
            if (afterColumn == null) {
                // Then the column to move comes first ...
                newColumns.put(columnToMove.name().toLowerCase(), columnToMove);
            }
            sortedColumns.forEach((key, defn) -> {
                newColumns.put(key, defn);
                if (defn == afterColumn) {
                    // We just added the column that came before, so add our column here ...
                    newColumns.put(columnToMove.name().toLowerCase(), columnToMove);
                }
            });
            sortedColumns = newColumns;
        }
        updatePositions();
        return this;
    }

    @Override
    public TableEditor renameColumn(String existingName, String newName) {
        final Column existing = columnWithName(existingName);
        if (existing == null) {
            throw new IllegalArgumentException("No column with name '" + existingName + "'");
        }
        Column newColumn = existing.edit().name(newName).create();
        // Determine the primary key names ...
        List<String> newPkNames = null;
        if (!hasUniqueValues() && primaryKeyColumnNames().contains(existing.name())) {
            newPkNames = new ArrayList<>(primaryKeyColumnNames());
            newPkNames.replaceAll(name -> existing.name().equals(name) ? newName : name);
        }
        // Add the new column, move it before the existing column, and remove the old column ...
        addColumn(newColumn);
        reorderColumn(newColumn.name(), existing.name());
        removeColumn(existing.name());
        if (newPkNames != null) {
            setPrimaryKeyNames(newPkNames);
        }
        return this;
    }

    protected void updatePositions() {
        AtomicInteger position = new AtomicInteger(1);
        sortedColumns.replaceAll((name, defn) -> {
            // Decrement the position ...
            int nextPosition = position.getAndIncrement();
            if (defn.position() != nextPosition) {
                return defn.edit().position(nextPosition).create();
            }
            return defn;
        });
    }

    protected boolean positionsAreValid() {
        AtomicInteger position = new AtomicInteger(1);
        return sortedColumns.values().stream().allMatch(defn -> defn.position() >= position.getAndSet(defn.position() + 1));
    }

    @Override
    public String toString() {
        return create().toString();
    }

    @Override
    public boolean chearConstraint() {
        this.pkColumnChanges.clear();
        this.checkColumns.clear();
        this.uniqueColumns.clear();
        this.fkColumns.clear();
        return Boolean.TRUE;
    }

    @Override
    public Table create() {
        if (id == null) {
            throw new IllegalStateException("Unable to create a table from an editor that has no table ID");
        }
        List<Column> columns = new ArrayList<>();
        sortedColumns.values().forEach(column -> {
            column = column.edit().charsetNameOfTable(defaultCharsetName).create();
            columns.add(column);
        });
        updatePrimaryKeys();
        return new TableImpl(id, columns, primaryKeyColumnNames(), primaryKeyColumnChanges(), constraintChanges(), foreignKeyColumns(),
                uniqueColumns, checkColumns, defaultCharsetName, comment);
    }
}
