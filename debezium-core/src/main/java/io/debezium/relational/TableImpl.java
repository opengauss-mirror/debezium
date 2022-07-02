/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import io.debezium.annotation.PackagePrivate;
import io.debezium.util.Strings;

/**
 * Modified by an in 2020.7.2 for constraint feature
 */
final class TableImpl implements Table {

    private final TableId id;
    private final List<Column> columnDefs;
    private final List<String> pkColumnNames;
    private final List<Map<String, String>> pkColumnChanges;
    private final List<Map<String, String>> constraintChanges;
    private final List<Map<String, String>> fkColumns;
    private final List<Map<String, String>> uniqueColumns;
    private final List<Map<String, String>> checkColumns;
    private final Map<String, Column> columnsByLowercaseName;
    private final String defaultCharsetName;
    private final String comment;

    @PackagePrivate
    TableImpl(Table table) {
        this(table.id(), table.columns(), table.primaryKeyColumnNames(), table.primaryKeyColumnChanges(), table.constraintChanges(), table.foreignKeyColumns(),
                table.uniqueColumns(), table.checkColumns(), table.defaultCharsetName(), table.comment());
    }

    @PackagePrivate
    TableImpl(TableId id, List<Column> sortedColumns, List<String> pkColumnNames, List<Map<String, String>> pkColumnChanges, List<Map<String, String>> constraintChanges,
              List<Map<String, String>> fkColumns,
              List<Map<String, String>> uniqueColumns, List<Map<String, String>> checkColumns, String defaultCharsetName, String comment) {
        this.id = id;
        this.columnDefs = Collections.unmodifiableList(sortedColumns);
        this.pkColumnNames = pkColumnNames == null ? Collections.emptyList() : Collections.unmodifiableList(pkColumnNames);
        this.pkColumnChanges = pkColumnNames == null ? Collections.emptyList() : Collections.unmodifiableList(pkColumnChanges);
        this.uniqueColumns = uniqueColumns == null ? Collections.emptyList() : Collections.unmodifiableList(uniqueColumns);
        this.checkColumns = checkColumns == null ? Collections.emptyList() : Collections.unmodifiableList(checkColumns);
        this.fkColumns = fkColumns == null ? Collections.emptyList() : Collections.unmodifiableList(fkColumns);
        Map<String, Column> defsByLowercaseName = new LinkedHashMap<>();
        for (Column def : this.columnDefs) {
            defsByLowercaseName.put(def.name().toLowerCase(), def);
        }
        this.columnsByLowercaseName = Collections.unmodifiableMap(defsByLowercaseName);
        this.defaultCharsetName = defaultCharsetName;
        this.comment = comment;
        this.constraintChanges = constraintChanges;
    }

    @Override
    public TableId id() {
        return id;
    }

    @Override
    public List<String> primaryKeyColumnNames() {
        return pkColumnNames;
    }

    @Override
    public List<Map<String, String>> primaryKeyColumnChanges() {
        return pkColumnChanges;
    }

    @Override
    public List<Map<String, String>> constraintChanges() {
        return constraintChanges;
    }

    @Override
    public List<Map<String, String>> foreignKeyColumns() {
        return fkColumns;
    }

    @Override
    public List<Map<String, String>> uniqueColumns() {
        return uniqueColumns;
    }

    @Override
    public List<Map<String, String>> checkColumns() {
        return checkColumns;
    }

    @Override
    public List<Column> columns() {
        return columnDefs;
    }

    @Override
    public List<String> retrieveColumnNames() {
        return columnDefs.stream()
                .map(Column::name)
                .collect(Collectors.toList());
    }

    @Override
    public Column columnWithName(String name) {
        return columnsByLowercaseName.get(name.toLowerCase());
    }

    @Override
    public String defaultCharsetName() {
        return defaultCharsetName;
    }

    @Override
    public String comment() {
        return comment;
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj instanceof Table) {
            Table that = (Table) obj;
            return this.id().equals(that.id())
                    && this.columns().equals(that.columns())
                    && this.primaryKeyColumnNames().equals(that.primaryKeyColumnNames())
                    && Strings.equalsIgnoreCase(this.defaultCharsetName(), that.defaultCharsetName());
        }
        return false;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        toString(sb, "");
        return sb.toString();
    }

    public void toString(StringBuilder sb, String prefix) {
        if (prefix == null) {
            prefix = "";
        }
        sb.append(prefix).append("columns: {").append(System.lineSeparator());
        for (Column defn : columnDefs) {
            sb.append(prefix).append("  ").append(defn).append(System.lineSeparator());
        }
        sb.append(prefix).append("}").append(System.lineSeparator());
        sb.append(prefix).append("primary key: ").append(primaryKeyColumnNames()).append(System.lineSeparator());
        sb.append(prefix).append("default charset: ").append(defaultCharsetName()).append(System.lineSeparator());
        sb.append(prefix).append("comment: ").append(comment()).append(System.lineSeparator());
    }

    @Override
    public TableEditor edit() {
        return new TableEditorImpl().tableId(id)
                .setColumns(columnDefs)
                .setPrimaryKeyNames(pkColumnNames)
                // .setUniqueValues()
                .setForeignKeys(fkColumns)
                .setPrimaryKeyChanges(pkColumnChanges)
                .setConstraintChanges(constraintChanges)
                .setUniqueColumns(uniqueColumns)
                .setCheckColumns(checkColumns)
                .setDefaultCharsetName(defaultCharsetName)
                .setComment(comment);
    }
}
