/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.antlr.listener;

import static io.debezium.antlr.AntlrDdlParser.getText;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.antlr.v4.runtime.tree.ParseTreeListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.oracle.antlr.OracleDdlParser;
import io.debezium.ddl.parser.oracle.generated.PlSqlParser;
import io.debezium.ddl.parser.oracle.generated.PlSqlParser.Constraint_nameContext;
import io.debezium.ddl.parser.oracle.generated.PlSqlParser.ExpressionContext;
import io.debezium.relational.Column;
import io.debezium.relational.ColumnEditor;
import io.debezium.relational.TableEditor;
import io.debezium.relational.TableId;
import io.debezium.text.ParsingException;
import io.netty.util.internal.StringUtil;

/**
 * Parser listener that is parsing Oracle ALTER TABLE statements
 * Modified by an in 2020.7.2 for constraint feature
 */
public class AlterTableParserListener extends BaseParserListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(AlterTableParserListener.class);

    private static final int STARTING_INDEX = 1;
    private TableEditor tableEditor;
    private String catalogName;
    private String schemaName;
    private TableId previousTableId;
    private OracleDdlParser parser;
    private final List<ParseTreeListener> listeners;
    private ColumnDefinitionParserListener columnDefinitionParserListener;
    private List<ColumnEditor> columnEditors;
    private int parsingColumnIndex = STARTING_INDEX;

    /**
     * Package visible Constructor
     *
     * @param catalogName Represents database name. If null, points to the current database
     * @param schemaName Schema/user name. If null, points to the current schema
     * @param parser Oracle Antlr parser
     * @param listeners registered listeners
     */
    AlterTableParserListener(final String catalogName, final String schemaName, final OracleDdlParser parser,
                             final List<ParseTreeListener> listeners) {
        this.catalogName = catalogName;
        this.schemaName = schemaName;
        this.parser = parser;
        this.listeners = listeners;
    }

    @Override
    public void enterAlter_table(PlSqlParser.Alter_tableContext ctx) {
        previousTableId = null;
        TableId tableId = new TableId(catalogName, schemaName, getTableName(ctx.tableview_name()));
        if (parser.databaseTables().forTable(tableId) == null) {
            LOGGER.debug("Ignoring ALTER TABLE statement for non-captured table {}", tableId);
            return;
        }
        tableEditor = parser.databaseTables().editTable(tableId);
        tableEditor.chearConstraint();
        List<Column> columns = tableEditor.columns();

        for (Column column : columns) {
            column.clearModifyKeys();
        }

        if (tableEditor == null) {
            throw new ParsingException(null, "Trying to alter table " + tableId.toString()
                    + ", which does not exist. Query: " + getText(ctx));
        }
        super.enterAlter_table(ctx);
    }

    @Override
    public void exitAlter_table(PlSqlParser.Alter_tableContext ctx) {
        parser.runIfNotNull(() -> {
            listeners.remove(columnDefinitionParserListener);
            parser.databaseTables().overwriteTable(tableEditor.create());
            parser.signalAlterTable(tableEditor.tableId(), previousTableId, ctx.getParent());
        }, tableEditor);
        super.exitAlter_table(ctx);
    }

    @Override
    public void enterAlter_table_properties(PlSqlParser.Alter_table_propertiesContext ctx) {
        parser.runIfNotNull(() -> {
            if (ctx.RENAME() != null && ctx.TO() != null) {
                previousTableId = tableEditor.tableId();
                String tableName = getTableName(ctx.tableview_name());
                final TableId newTableId = new TableId(tableEditor.tableId().catalog(), tableEditor.tableId().schema(), tableName);
                if (parser.getTableFilter().isIncluded(previousTableId) && !parser.getTableFilter().isIncluded(newTableId)) {
                    LOGGER.warn("Renaming included table {} to non-included table {}, this can lead to schema inconsistency", previousTableId, newTableId);
                }
                else if (!parser.getTableFilter().isIncluded(previousTableId) && parser.getTableFilter().isIncluded(newTableId)) {
                    LOGGER.warn("Renaming non-included table {} to included table {}, this can lead to schema inconsistency", previousTableId, newTableId);
                }
                parser.databaseTables().overwriteTable(tableEditor.create());
                parser.databaseTables().renameTable(tableEditor.tableId(), newTableId);
                tableEditor = parser.databaseTables().editTable(newTableId);
            }
        }, tableEditor);
        super.exitAlter_table_properties(ctx);
    }

    @Override
    public void enterAdd_column_clause(PlSqlParser.Add_column_clauseContext ctx) {
        parser.runIfNotNull(() -> {
            List<PlSqlParser.Column_definitionContext> columns = ctx.column_definition();
            columnEditors = new ArrayList<>(columns.size());
            for (PlSqlParser.Column_definitionContext column : columns) {
                String columnName = getColumnName(column.column_name());
                ColumnEditor editor = Column.editor().name(columnName);
                columnEditors.add(editor);
            }
            columnDefinitionParserListener = new ColumnDefinitionParserListener(tableEditor, columnEditors.get(0), parser, listeners);
            listeners.add(columnDefinitionParserListener);
        }, tableEditor);
        super.enterAdd_column_clause(ctx);
    }

    @Override
    public void enterModify_column_clauses(PlSqlParser.Modify_column_clausesContext ctx) {
        parser.runIfNotNull(() -> {
            List<PlSqlParser.Modify_col_propertiesContext> columns = ctx.modify_col_properties();
            columnEditors = new ArrayList<>(columns.size());
            for (PlSqlParser.Modify_col_propertiesContext column : columns) {
                String columnName = getColumnName(column.column_name());
                Column existingColumn = tableEditor.columnWithName(columnName);
                if (existingColumn != null) {
                    ColumnEditor columnEditor = existingColumn.edit();
                    columnEditors.add(columnEditor);
                }
                else {
                    throw new ParsingException(null, "trying to change column " + columnName + " in " +
                            tableEditor.tableId().toString() + " table, which does not exist.  Query: " + getText(ctx));
                }

                modify_column_clauses(column, columnName);
            }
            columnDefinitionParserListener = new ColumnDefinitionParserListener(tableEditor, columnEditors.get(0), parser, listeners);
            listeners.add(columnDefinitionParserListener);
        }, tableEditor);
        super.enterModify_column_clauses(ctx);
    }

    @Override
    public void exitAdd_column_clause(PlSqlParser.Add_column_clauseContext ctx) {
        parser.runIfNotNull(() -> {
            columnEditors.forEach(columnEditor -> tableEditor.addColumn(columnEditor.create()));
            listeners.remove(columnDefinitionParserListener);
            columnDefinitionParserListener = null;
        }, tableEditor, columnEditors);
        super.exitAdd_column_clause(ctx);
    }

    @Override
    public void exitModify_column_clauses(PlSqlParser.Modify_column_clausesContext ctx) {
        parser.runIfNotNull(() -> {
            columnEditors.forEach(columnEditor -> tableEditor.addColumn(columnEditor.create()));
            listeners.remove(columnDefinitionParserListener);
            columnDefinitionParserListener = null;
        }, tableEditor, columnEditors);
        super.exitModify_column_clauses(ctx);
    }

    @Override
    public void exitColumn_definition(PlSqlParser.Column_definitionContext ctx) {
        parser.runIfNotNull(() -> {
            if (columnEditors != null) {
                // column editor list is not null when a multiple columns are parsed in one statement
                if (columnEditors.size() > parsingColumnIndex) {
                    // assign next column editor to parse another column definition
                    columnDefinitionParserListener.setColumnEditor(columnEditors.get(parsingColumnIndex++));
                }
                else {
                    // all columns parsed
                    // reset global variables for next parsed statement
                    columnEditors.forEach(columnEditor -> tableEditor.addColumn(columnEditor.create()));
                    columnEditors = null;
                    parsingColumnIndex = STARTING_INDEX;
                }
            }
        }, tableEditor, columnEditors);
        super.exitColumn_definition(ctx);
    }

    @Override
    public void exitModify_col_properties(PlSqlParser.Modify_col_propertiesContext ctx) {
        parser.runIfNotNull(() -> {
            if (columnEditors != null) {
                // column editor list is not null when multiple columns are paresd in one statement
                if (columnEditors.size() > parsingColumnIndex) {
                    // assign next column editor to parse another column definition
                    columnDefinitionParserListener.setColumnEditor(columnEditors.get(parsingColumnIndex++));
                }
                else {
                    // all columns parsed
                    // reset global variables for next parsed statement
                    columnEditors.forEach(columnEditor -> tableEditor.addColumn(columnEditor.create()));
                    columnEditors = null;
                    parsingColumnIndex = STARTING_INDEX;
                }
            }
        }, tableEditor, columnEditors);
        super.exitModify_col_properties(ctx);
    }

    @Override
    public void enterDrop_column_clause(PlSqlParser.Drop_column_clauseContext ctx) {
        parser.runIfNotNull(() -> {
            List<PlSqlParser.Column_nameContext> columnNameContexts = ctx.column_name();
            columnEditors = new ArrayList<>(columnNameContexts.size());
            for (PlSqlParser.Column_nameContext columnNameContext : columnNameContexts) {
                String columnName = getColumnName(columnNameContext);
                tableEditor.removeColumn(columnName);
            }
        }, tableEditor);
        super.enterDrop_column_clause(ctx);
    }

    @Override
    public void exitRename_column_clause(PlSqlParser.Rename_column_clauseContext ctx) {
        parser.runIfNotNull(() -> {
            tableEditor.renameColumn(getColumnName(ctx.old_column_name()), getColumnName(ctx.new_column_name()));
        }, tableEditor);
        super.exitRename_column_clause(ctx);
    }

    @Override
    public void enterConstraint_clauses(PlSqlParser.Constraint_clausesContext ctx) {
        parser.runIfNotNull(() -> {
            if (ctx.ADD() != null) {
                // ALTER TABLE ADD PRIMARY KEY
                List<String> primaryKeyColumns = new ArrayList<>();
                List<Map<String, String>> pkColumnChanges = new ArrayList<>();
                List<Map<String, String>> checkColumns = new ArrayList<Map<String, String>>();
                List<Map<String, String>> uniqueColumns = new ArrayList<Map<String, String>>();
                List<Map<String, String>> constraintChanges = new ArrayList<Map<String, String>>();

                for (PlSqlParser.Out_of_line_constraintContext constraint : ctx.out_of_line_constraint()) {
                    if (constraint.PRIMARY() != null) {
                        Constraint_nameContext constraint_name = constraint.constraint_name();
                        for (PlSqlParser.Column_nameContext columnNameContext : constraint.column_name()) {
                            Map<String, String> pkChangeMap = new HashMap<>();
                            primaryKeyColumns.add(getColumnName(columnNameContext));

                            pkChangeMap.put(COLUMN_NAME, getColumnName(columnNameContext));
                            pkChangeMap.put(PRIMARY_KEY_ACTION, ctx.ADD().getText());
                            if (constraint_name != null) {
                                final Map<String, String> constraintColumn = new HashMap<>();
                                pkChangeMap.put(CONSTRAINT_NAME, constraint_name.getText());
                                constraintColumn.put(CONSTRAINT_NAME, constraint_name.getText());
                                constraintColumn.put(TYPE_NAME, constraint.PRIMARY().getText());
                                constraintChanges.add(constraintColumn);
                            }

                            pkColumnChanges.add(pkChangeMap);
                        }
                    }
                    else if (constraint.UNIQUE() != null) {
                        Constraint_nameContext constraint_name = constraint.constraint_name();

                        for (PlSqlParser.Column_nameContext columnNameContext : constraint.column_name()) {
                            final Map<String, String> uniqueColumn = new HashMap<>();
                            if (constraint_name != null) {
                                uniqueColumn.put(INDEX_NAME, constraint_name.getText());
                            }
                            else {
                                continue;
                            }
                            uniqueColumn.put(COLUMN_NAME, getColumnName(columnNameContext));
                            uniqueColumns.add(uniqueColumn);
                        }

                    }
                    else if (constraint.CHECK() != null) {
                        ExpressionContext expression = constraint.expression();
                        if (expression != null) {
                            final Map<String, String> checkColumn = new HashMap<>();
                            Constraint_nameContext constraint_name = constraint.constraint_name();
                            if (constraint_name != null) {
                                checkColumn.put(INDEX_NAME, constraint_name.getText());
                            }
                            else {
                                continue;
                            }

                            String condition = Logical_expression_parse(expression.logical_expression());
                            checkColumn.put(CONDITION, condition);
                            checkColumns.add(checkColumn);

                        }
                    }

                }
                if (!primaryKeyColumns.isEmpty()) {
                    tableEditor.setPrimaryKeyNames(primaryKeyColumns);
                }
                if (!pkColumnChanges.isEmpty()) {
                    tableEditor.setPrimaryKeyChanges(pkColumnChanges);
                }
                if (!checkColumns.isEmpty()) {
                    tableEditor.setCheckColumns(checkColumns);
                }
                if (!uniqueColumns.isEmpty()) {
                    tableEditor.setUniqueColumns(uniqueColumns);
                }
                if (!constraintChanges.isEmpty()) {
                    tableEditor.setConstraintChanges(constraintChanges);
                }
            }
            else if (ctx.MODIFY() != null && ctx.PRIMARY() != null && ctx.KEY() != null) {
                // ALTER TABLE MODIFY PRIMARY KEY columns
                List<String> primaryKeyColumns = new ArrayList<>();
                for (PlSqlParser.Column_nameContext columnNameContext : ctx.column_name()) {
                    primaryKeyColumns.add(getColumnName(columnNameContext));
                }
                if (!primaryKeyColumns.isEmpty()) {
                    tableEditor.setPrimaryKeyNames(primaryKeyColumns);
                }
            }
            else if (ctx.drop_constraint_clause() != null) {
                List<PlSqlParser.Drop_constraint_clauseContext> dropConstraintList = ctx.drop_constraint_clause();
                List<Map<String, String>> pkColumnChanges = new ArrayList<>();
                for (PlSqlParser.Drop_constraint_clauseContext dropConstraint : dropConstraintList) {
                    PlSqlParser.Drop_primary_key_or_unique_or_generic_clauseContext dropPrimaryKeyOrUnique = dropConstraint
                            .drop_primary_key_or_unique_or_generic_clause();

                    if (dropConstraint.DROP() != null) {
                        Constraint_nameContext constraint_name = dropPrimaryKeyOrUnique.constraint_name();

                        List<String> primaryKeyColumnNames = tableEditor.primaryKeyColumnNames();

                        for (String primaryKeyColumnName : primaryKeyColumnNames) {
                            Map<String, String> pkChangeMap = new HashMap<>();
                            pkChangeMap.put(COLUMN_NAME, primaryKeyColumnName);
                            pkChangeMap.put(PRIMARY_KEY_ACTION, dropConstraint.DROP().getText());
                            pkChangeMap.put(PRIMARY_DROP_IS_CASCADE, dropPrimaryKeyOrUnique.CASCADE() == null ? StringUtil.EMPTY_STRING : PRIMARY_DROP_IS_CASCADE);
                            if (constraint_name != null) {
                                String constraintName = constraint_name.getText();
                                pkChangeMap.put(CONSTRAINT_NAME, constraintName);
                                pkChangeMap.put(TYPE_NAME, removeConstraintAndGetType(constraintName).toString());
                            }
                            pkColumnChanges.add(pkChangeMap);
                        }
                    }

                }
                if (!pkColumnChanges.isEmpty()) {
                    tableEditor.setPrimaryKeyChanges(pkColumnChanges);
                }
            }
        }, tableEditor);
        super.enterConstraint_clauses(ctx);
    }

    private Integer removeConstraintAndGetType(String constraintName) {
        Iterator<Map<String, String>> constraintIterator = tableEditor.constraintChanges().iterator();

        while (constraintIterator.hasNext()) {
            Map<String, String> constraint = constraintIterator.next();
            String conName = constraint.get(CONSTRAINT_NAME);
            if (conName != null && conName.equals(constraintName)) {
                constraintIterator.remove();
                return 1;
            }
        }
        return 0;
    }


    private void modify_column_clauses(PlSqlParser.Modify_col_propertiesContext column, String columnName) {
        // ALTER TABLE Modify constraint
        List<String> primaryKeyColumns = new ArrayList<>();
        List<Map<String, String>> pkColumnChanges = new ArrayList<>();
        List<Map<String, String>> checkColumns = new ArrayList<Map<String, String>>();
        List<Map<String, String>> uniqueColumns = new ArrayList<Map<String, String>>();
        List<Map<String, String>> constraintChanges = new ArrayList<Map<String, String>>();

        for(PlSqlParser.Inline_constraintContext constraint : column.inline_constraint()) {
            if (constraint.PRIMARY() != null) {
                Constraint_nameContext constraint_name = constraint.constraint_name();
                Map<String, String> pkChangeMap = new HashMap<>();
                primaryKeyColumns.add(columnName);

                pkChangeMap.put(COLUMN_NAME, columnName);
                pkChangeMap.put(PRIMARY_KEY_ACTION, PRIMARY_KEY_ADD);
                if (constraint_name != null) {
                    final Map<String, String> constraintColumn = new HashMap<>();
                    pkChangeMap.put(CONSTRAINT_NAME, constraint_name.getText());
                    constraintColumn.put(CONSTRAINT_NAME, constraint_name.getText());
                    constraintColumn.put(TYPE_NAME, constraint.PRIMARY().getText());
                    constraintChanges.add(constraintColumn);
                }

                pkColumnChanges.add(pkChangeMap);
            }
            else if (constraint.UNIQUE() != null) {
                Constraint_nameContext constraint_name = constraint.constraint_name();

                final Map<String, String> uniqueColumn = new HashMap<>();
                if (constraint_name != null) {
                    uniqueColumn.put(INDEX_NAME, constraint_name.getText());
                }
                uniqueColumn.put(COLUMN_NAME, columnName);
                uniqueColumns.add(uniqueColumn);

            }
            else if (constraint.check_constraint() != null) {
                ExpressionContext expression = constraint.check_constraint().condition().expression();
                if (expression != null) {
                    final Map<String, String> checkColumn = new HashMap<>();
                    Constraint_nameContext constraint_name = constraint.constraint_name();
                    if (constraint_name != null) {
                        checkColumn.put(INDEX_NAME, constraint_name.getText());
                    }

                    String condition = Logical_expression_parse(expression.logical_expression());
                    checkColumn.put(CONDITION, condition);
                    checkColumns.add(checkColumn);

                }
            } else if (constraint.references_clause() != null) {
                List<Map<String, String>> fkColumns = enterInline_ref_constraint(constraint.references_clause(),
                    tableEditor.tableId().schema(), columnName, constraint.constraint_name());
                tableEditor.setForeignKeys(fkColumns);

            }
        }
        if (!primaryKeyColumns.isEmpty()) {
            tableEditor.setPrimaryKeyNames(primaryKeyColumns);
        }
        if (!pkColumnChanges.isEmpty()) {
            tableEditor.setPrimaryKeyChanges(pkColumnChanges);
        }
        if (!checkColumns.isEmpty()) {
            tableEditor.setCheckColumns(checkColumns);
        }
        if (!uniqueColumns.isEmpty()) {
            tableEditor.setUniqueColumns(uniqueColumns);
        }
        if (!constraintChanges.isEmpty()) {
            tableEditor.setConstraintChanges(constraintChanges);
        }
    }
}
