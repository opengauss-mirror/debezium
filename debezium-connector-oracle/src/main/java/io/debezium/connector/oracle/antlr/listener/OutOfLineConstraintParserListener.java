/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.antlr.listener;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.antlr.v4.runtime.tree.ParseTreeListener;
import org.antlr.v4.runtime.tree.TerminalNode;

import io.debezium.antlr.DataTypeResolver;
import io.debezium.connector.oracle.antlr.OracleDdlParser;
import io.debezium.ddl.parser.oracle.generated.PlSqlParser;
import io.debezium.relational.TableEditor;
import io.netty.util.internal.StringUtil;

/**
 * @author saxisuer
 */
public class OutOfLineConstraintParserListener extends BaseParserListener {
    private final OracleDdlParser parser;
    private final DataTypeResolver dataTypeResolver;
    private final TableEditor tableEditor;
    private final List<ParseTreeListener> listeners;

    public OutOfLineConstraintParserListener(OracleDdlParser parser, TableEditor tableEditor, List<ParseTreeListener> listeners) {
        this.parser = parser;
        this.dataTypeResolver = parser.dataTypeResolver();
        this.tableEditor = tableEditor;
        this.listeners = listeners;
    }

    @Override
    public void enterOut_of_line_constraint(PlSqlParser.Out_of_line_constraintContext constraint) {
        LOGGER.debug("enterOut_of_line_constraint");
        // ALTER TABLE ADD PRIMARY KEY
        List<String> primaryKeyColumns = new ArrayList<>();
        List<Map<String, String>> pkColumnChanges = new ArrayList<>();
        List<Map<String, String>> checkColumns = new ArrayList<>();
        List<Map<String, String>> uniqueColumns = new ArrayList<>();
        List<Map<String, String>> constraintChanges = new ArrayList<>();
        List<Map<String, String>> fkColumns = new ArrayList<>();
        if (constraint.PRIMARY() != null) {
            doPrimaryConstraint(constraint, primaryKeyColumns, pkColumnChanges, constraintChanges);
        }
        else if (constraint.UNIQUE() != null) {
            doUniqueConstraint(constraint, uniqueColumns);
        }
        else if (constraint.CHECK() != null) {
            doCheckConstraint(constraint, checkColumns);
        }
        PlSqlParser.Foreign_key_clauseContext foreign_key_clause = constraint.foreign_key_clause();
        if (foreign_key_clause != null) {
            PlSqlParser.Constraint_nameContext constraint_name = constraint.constraint_name();
            fkColumns = enterForeign_key_clause(foreign_key_clause, constraint_name);
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
        if (!fkColumns.isEmpty()) {
            tableEditor.setForeignKeys(fkColumns);
        }
        super.enterOut_of_line_constraint(constraint);
    }

    private void doCheckConstraint(PlSqlParser.Out_of_line_constraintContext constraint, List<Map<String, String>> checkColumns) {
        PlSqlParser.ExpressionContext expression = constraint.expression();
        if (expression != null) {
            final Map<String, String> checkColumn = new HashMap<>();
            PlSqlParser.Constraint_nameContext constraint_name = constraint.constraint_name();
            if (constraint_name != null) {
                checkColumn.put(INDEX_NAME, getTableOrColumnName(constraint_name.getText()));
            }
            List<String> includeColumn = Logical_expression_parse(expression.logical_expression());
            String rawExpr = OracleDdlParser.getText(expression.logical_expression());
            for (int i = 0; i < includeColumn.size(); i++) {
                rawExpr = rawExpr.replace(includeColumn.get(i), ":$" + i);
            }
            checkColumn.put(CONDITION, rawExpr);
            checkColumn.put(INCLUDE_COLUMN, includeColumn.stream().map(BaseParserListener::getTableOrColumnName).collect(Collectors.joining(
                    ",")));
            checkColumns.add(checkColumn);
        }
    }

    private void doUniqueConstraint(PlSqlParser.Out_of_line_constraintContext constraint, List<Map<String, String>> uniqueColumns) {
        uniqueColumns.addAll(tableEditor.uniqueColumns());
        PlSqlParser.Constraint_nameContext constraint_name = constraint.constraint_name();

        for (PlSqlParser.Column_nameContext columnNameContext : constraint.column_name()) {
            final Map<String, String> uniqueColumn = new HashMap<>();
            if (constraint_name != null) {
                uniqueColumn.put(INDEX_NAME, getTableOrColumnName(constraint_name.getText()));
            }
            else {
                continue;
            }
            uniqueColumn.put(COLUMN_NAME, getColumnName(columnNameContext));
            uniqueColumns.add(uniqueColumn);
        }
    }

    private void doPrimaryConstraint(PlSqlParser.Out_of_line_constraintContext constraint, List<String> primaryKeyColumns,
                                     List<Map<String, String>> pkColumnChanges, List<Map<String, String>> constraintChanges) {
        PlSqlParser.Constraint_nameContext constraint_name = constraint.constraint_name();
        for (PlSqlParser.Column_nameContext columnNameContext : constraint.column_name()) {
            Map<String, String> pkChangeMap = new HashMap<>();
            primaryKeyColumns.add(getColumnName(columnNameContext));

            pkChangeMap.put(COLUMN_NAME, getColumnName(columnNameContext));
            if (constraint.getParent() instanceof PlSqlParser.Constraint_clausesContext) {
                TerminalNode add = ((PlSqlParser.Constraint_clausesContext) constraint.getParent()).ADD();
                if (add != null) {
                    pkChangeMap.put(PRIMARY_KEY_ACTION, add.getText());
                }
            }
            else {
                pkChangeMap.put(PRIMARY_KEY_ACTION, constraint.getText());
            }
            if (constraint_name != null) {
                final Map<String, String> constraintColumn = new HashMap<>();
                pkChangeMap.put(CONSTRAINT_NAME, getTableOrColumnName(constraint_name.getText()));
                constraintColumn.put(CONSTRAINT_NAME, getTableOrColumnName(constraint_name.getText()));
                constraintColumn.put(TYPE_NAME, constraint.PRIMARY().getText());
                constraintChanges.add(constraintColumn);
                List<String> primaryConstraintName = new ArrayList<>();
                primaryConstraintName.add(getTableOrColumnName(constraint_name.getText()));
                tableEditor.setPrimaryConstraintName(primaryConstraintName);
            }

            pkColumnChanges.add(pkChangeMap);
        }
    }

    public List<Map<String, String>> enterForeign_key_clause(PlSqlParser.Foreign_key_clauseContext ctx,
                                                             PlSqlParser.Constraint_nameContext constraint_name) {

        List<Map<String, String>> fkColumns = new ArrayList<>();
        final Map<String, String> pkColumn = new HashMap<>();
        List<PlSqlParser.Column_nameContext> parens = ctx.paren_column_list().column_list().column_name();
        List<PlSqlParser.Column_nameContext> references = ctx.references_clause().paren_column_list().column_list().column_name();
        String[] tableId = ctx.references_clause().tableview_name().getText().split(DOT);
        String schema = tableEditor.tableId().schema();
        pkColumn.put(PKTABLE_SCHEM, getAttribute(tableId.length > 1 ? tableId[0] : schema));
        pkColumn.put(PKTABLE_NAME, getAttribute(tableId.length > 1 ? tableId[1] : tableId[0]));
        List<String> pkColumnList = references.stream().map(reference -> getAttribute(reference.getText())).collect(Collectors.toList());
        List<String> fkColumnList = parens.stream().map(paren -> getAttribute(paren.getText())).collect(Collectors.toList());
        pkColumn.put(PKCOLUMN_NAME, StringUtil.join(String.valueOf(StringUtil.COMMA), pkColumnList).toString());
        pkColumn.put(FKCOLUMN_NAME, StringUtil.join(String.valueOf(StringUtil.COMMA), fkColumnList).toString());
        pkColumn.put(FK_NAME,
                constraint_name != null ? getTableOrColumnName(constraint_name.getText())
                        : buildInlineFkName(pkColumn.get(PKTABLE_SCHEM),
                                pkColumn.get(PKTABLE_NAME),
                                pkColumn.get(FKCOLUMN_NAME),
                                pkColumn.get(PKCOLUMN_NAME)));

        if (ctx.on_delete_clause() != null) {
            pkColumn.put(FK_DROP_IS_CASCADE, on_delete_clause(ctx.on_delete_clause()));
        }

        fkColumns.add(pkColumn);
        return fkColumns;
    }

    private String on_delete_clause(PlSqlParser.On_delete_clauseContext ctx) {
        StringBuilder sb = new StringBuilder();
        if (ctx.ON() != null) {
            sb.append(ctx.ON().getText()).append(SPACE);
        }
        if (ctx.DELETE() != null) {
            sb.append(ctx.DELETE().getText()).append(SPACE);
        }
        if (ctx.CASCADE() != null) {
            sb.append(ctx.CASCADE().getText()).append(SPACE);
        }
        if (ctx.SET() != null) {
            sb.append(ctx.SET().getText()).append(SPACE);
        }
        if (ctx.NULL_() != null) {
            sb.append(ctx.NULL_().getText()).append(SPACE);
        }
        return sb.toString();
    }
}
