/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.antlr.listener;

import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.antlr.v4.runtime.tree.ParseTreeListener;

import io.debezium.antlr.DataTypeResolver;
import io.debezium.connector.oracle.antlr.OracleDdlParser;
import io.debezium.ddl.parser.oracle.generated.PlSqlParser;
import io.debezium.ddl.parser.oracle.generated.PlSqlParser.Check_constraintContext;
import io.debezium.ddl.parser.oracle.generated.PlSqlParser.Column_nameContext;
import io.debezium.ddl.parser.oracle.generated.PlSqlParser.ExpressionContext;
import io.debezium.ddl.parser.oracle.generated.PlSqlParser.Constraint_nameContext;
import io.debezium.ddl.parser.oracle.generated.PlSqlParser.Foreign_key_clauseContext;
import io.debezium.ddl.parser.oracle.generated.PlSqlParser.Inline_constraintContext;
import io.debezium.ddl.parser.oracle.generated.PlSqlParser.References_clauseContext;
import io.debezium.relational.Column;
import io.debezium.relational.ColumnEditor;
import io.debezium.relational.TableEditor;
import io.netty.util.internal.StringUtil;

import oracle.jdbc.OracleTypes;

/**
 * Parser listener that parses column definitions of Oracle DDL statements.
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 * Modified by an in 2020.7.2 for constraint feature
 */
public class ColumnDefinitionParserListener extends BaseParserListener {

    private final OracleDdlParser parser;
    private final DataTypeResolver dataTypeResolver;
    private final TableEditor tableEditor;
    private final List<ParseTreeListener> listeners;
    private ColumnEditor columnEditor;

    ColumnDefinitionParserListener(final TableEditor tableEditor, final ColumnEditor columnEditor, OracleDdlParser parser,
                                   List<ParseTreeListener> listeners) {
        this.tableEditor = tableEditor;
        this.columnEditor = columnEditor;
        this.parser = parser;
        this.dataTypeResolver = parser.dataTypeResolver();
        this.listeners = listeners;
    }

    void setColumnEditor(ColumnEditor columnEditor) {
        this.columnEditor = columnEditor;
    }

    Column getColumn() {
        return columnEditor.create();
    }

    @Override
    public void enterColumn_definition(PlSqlParser.Column_definitionContext ctx) {
        resolveColumnDataType(ctx);
        if (ctx.DEFAULT() != null) {
            columnEditor.defaultValueExpression(ctx.column_default_value().getText());
        }

        List<Inline_constraintContext> inline_constraint = ctx.inline_constraint();
        if (inline_constraint != null && inline_constraint.size() > 0) {

            String indexName = ctx.type_name() != null ? ctx.type_name().getText() : "CHECK_" + ctx.column_name().getText().replace("_", "").toUpperCase();

            String columnName = ctx.column_name().getText();

            inline_constraint.forEach(inlineConstraint -> enterInline_constraint(inlineConstraint, indexName, columnName));
        }
        super.enterColumn_definition(ctx);
    }

    @Override
    public void enterPrimary_key_clause(PlSqlParser.Primary_key_clauseContext ctx) {
        // this rule will be parsed only if no primary key is set in a table
        // otherwise the statement can't be executed due to multiple primary key error
        columnEditor.optional(false);
        tableEditor.addColumn(columnEditor.create());
        tableEditor.setPrimaryKeyNames(columnEditor.name());
        super.enterPrimary_key_clause(ctx);
    }

    @Override
    public void enterOut_of_line_constraint(PlSqlParser.Out_of_line_constraintContext ctx) {
        // unique index set
        if (ctx.UNIQUE() != null) {

            List<Map<String, String>> uniqueColumns = new ArrayList<Map<String, String>>();

            final Map<String, String> uniqueColumn = new HashMap<>();
            uniqueColumn.put(INDEX_NAME, ctx.constraint_name().getText());
            ctx.column_name().forEach(columnName -> uniqueColumn.put(COLUMN_NAME, getAttribute(columnName.getText())));

            uniqueColumns.add(uniqueColumn);

            tableEditor.setUniqueColumns(uniqueColumns);
        }

        if (ctx.PRIMARY() != null) {
            List<Map<String, String>> constraintChanges = new ArrayList<Map<String, String>>();
            List<Map<String, String>> pkColumnChanges = new ArrayList<Map<String, String>>();
            ctx.column_name().forEach(columnName -> {
                final Map<String, String> pkColumn = new HashMap<>();
                pkColumn.put(COLUMN_NAME, getAttribute(columnName.getText()));
                pkColumn.put(PRIMARY_KEY_ACTION, PRIMARY_KEY_ADD);
                Constraint_nameContext constraint_name = ctx.constraint_name();
                if (constraint_name != null) {
                    pkColumn.put(CONSTRAINT_NAME, constraint_name.getText());
                    final Map<String, String> constraintColumn = new HashMap<>();
                    constraintColumn.put(CONSTRAINT_NAME, constraint_name.getText());
                    constraintColumn.put(TYPE_NAME, ctx.PRIMARY().getText());
                    constraintChanges.add(constraintColumn);
                }
                pkColumnChanges.add(pkColumn);
            });
            tableEditor.setConstraintChanges(constraintChanges);
            tableEditor.setPrimaryKeyChanges(pkColumnChanges);
        }

        // foreign_key set
        Foreign_key_clauseContext foreign_key_clause = ctx.foreign_key_clause();

        if (foreign_key_clause != null) {
            Constraint_nameContext constraint_name = ctx.constraint_name();
            enterForeign_key_clause(foreign_key_clause, constraint_name == null ? null : constraint_name.getText());
        }

        //check
        if (ctx.CHECK() != null) {
            String constraintName = ctx.constraint_name().getText();
            ExpressionContext expression = ctx.expression();

            List<Map<String, String>> checkColumns = new ArrayList<Map<String, String>>();

            final Map<String, String> checkColumn = new HashMap<>();
            checkColumn.put(INDEX_NAME, constraintName);

            String condition = Logical_expression_parse(expression.logical_expression());
            checkColumn.put(CONDITION, condition);

            checkColumns.add(checkColumn);

            tableEditor.setCheckColumns(checkColumns);
        }

        super.enterOut_of_line_constraint(ctx);
    }

    private void enterInline_constraint(PlSqlParser.Inline_constraintContext ctx, String indexName, String columnName) {

        Check_constraintContext check_constraint = ctx.check_constraint();
        References_clauseContext references_clause = ctx.references_clause();

        if (ctx.check_constraint() != null) {
            List<Map<String, String>> checkColumns = new ArrayList<Map<String, String>>();

            final Map<String, String> checkColumn = new HashMap<>();
            checkColumn.put(INDEX_NAME, indexName);

            String condition = enterCheck_constraint_condition(check_constraint);
            checkColumn.put(CONDITION, condition);

            checkColumns.add(checkColumn);

            tableEditor.setCheckColumns(checkColumns);

        }

        if (references_clause != null) {

            enterInline_ref_constraint(references_clause, columnName);

        }

        if (!StringUtil.isNullOrEmpty(ctx.getText()) && ctx.getText().equals(UNIQUE_NAME)) {
            List<Map<String, String>> uniqueColumns = new ArrayList<>();
            uniqueColumns.addAll(tableEditor.uniqueColumns());

            Map<String, String> uniqueColumn = new HashMap<>();
            uniqueColumn.put(INDEX_NAME, UNIQUE_NAME + "_" + columnName + "_key");
            uniqueColumn.put(COLUMN_NAME, columnName);
            uniqueColumns.add(uniqueColumn);

            tableEditor.setUniqueColumns(uniqueColumns);
        }

        if (ctx.PRIMARY() != null) {
            List<Map<String, String>> pkColumnChanges = new ArrayList<Map<String, String>>();
            List<Map<String, String>> constraintChanges = new ArrayList<Map<String, String>>();
            final Map<String, String> pkColumn = new HashMap<>();
            pkColumn.put(COLUMN_NAME, columnName);
            pkColumn.put(PRIMARY_KEY_ACTION, PRIMARY_KEY_ADD);
            Constraint_nameContext constraint_name = ctx.constraint_name();
            if (constraint_name != null) {
                pkColumn.put(CONSTRAINT_NAME, constraint_name.getText());
                final Map<String, String> constraintColumn = new HashMap<>();
                constraintColumn.put(CONSTRAINT_NAME, constraint_name.getText());
                constraintColumn.put(TYPE_NAME, ctx.PRIMARY().getText());
                constraintChanges.add(constraintColumn);
            }
            pkColumnChanges.add(pkColumn);
            tableEditor.setConstraintChanges(constraintChanges);
            tableEditor.setPrimaryKeyChanges(pkColumnChanges);
        }

        super.enterInline_constraint(ctx);
    }

    private String enterCheck_constraint_condition(PlSqlParser.Check_constraintContext ctx) {
        StringBuilder sb = new StringBuilder();
        sb.append(Logical_expression_parse(ctx.condition().expression().logical_expression()));

        return sb.toString();
    }

    public void enterInline_ref_constraint(PlSqlParser.References_clauseContext ctx, String columnName) {

        List<Map<String, String>> fkColumns = new ArrayList<Map<String, String>>();

        final Map<String, String> pkColumn = new HashMap<>();

        List<Column_nameContext> references = ctx.paren_column_list()
                .column_list().column_name();

        String[] tableId = ctx.tableview_name().getText().split(DOT);

        String schema = tableEditor.tableId().schema();

        pkColumn.put(PKTABLE_SCHEM, getAttribute(tableId.length > 1 ? tableId[0] : schema));
        pkColumn.put(PKTABLE_NAME, getAttribute(tableId.length > 1 ? tableId[1] : tableId[0]));

        for (int i = 0; i < references.size(); i++) {
            pkColumn.put(PKCOLUMN_NAME, getAttribute(references.get(i).getText()));
            pkColumn.put(FKCOLUMN_NAME, getAttribute(columnName));
            fkColumns.add(pkColumn);
        }

        pkColumn.put(FK_NAME, buildInlineFkName(pkColumn.get(PKTABLE_SCHEM),
                pkColumn.get(PKTABLE_NAME), columnName, pkColumn.get(PKCOLUMN_NAME)));

        tableEditor.setForeignKeys(fkColumns);
    }

    private String buildInlineFkName(String pkScheme, String pkTableName, String pkColumnName, String fkColumnName) {
        StringBuilder sb = new StringBuilder();
        sb.append("SYS_FK_");
        sb.append(pkScheme == null ? StringUtil.EMPTY_STRING : pkScheme.replaceAll("#", "_"));
        sb.append("_").append(pkTableName);
        sb.append("_").append(pkColumnName.replaceAll(String.valueOf(StringUtil.COMMA), "_"));
        sb.append("_").append(fkColumnName.replaceAll(String.valueOf(StringUtil.COMMA), "_"));
        return sb.toString();

    }

    public void enterForeign_key_clause(PlSqlParser.Foreign_key_clauseContext ctx, String fkName) {

        List<Map<String, String>> fkColumns = new ArrayList<Map<String, String>>();

        final Map<String, String> pkColumn = new HashMap<>();

        List<Column_nameContext> parens = ctx.paren_column_list().column_list().column_name();

        List<Column_nameContext> references = ctx.references_clause().paren_column_list()
                .column_list().column_name();

        String[] tableId = ctx.references_clause().tableview_name().getText().split(DOT);

        String schema = tableEditor.tableId().schema();

        pkColumn.put(PKTABLE_SCHEM, getAttribute(tableId.length > 1 ? tableId[0] : schema));
        pkColumn.put(PKTABLE_NAME, getAttribute(tableId.length > 1 ? tableId[1] : tableId[0]));

        List<String> pkColumnList = references.stream().map(reference -> getAttribute(reference.getText())).collect(Collectors.toList());
        List<String> fkColumnList = parens.stream().map(paren -> getAttribute(paren.getText())).collect(Collectors.toList());

        pkColumn.put(PKCOLUMN_NAME, StringUtil.join(String.valueOf(StringUtil.COMMA), pkColumnList).toString());
        pkColumn.put(FKCOLUMN_NAME, StringUtil.join(String.valueOf(StringUtil.COMMA), fkColumnList).toString());

        pkColumn.put(FK_NAME, fkName != null ? fkName
                : buildInlineFkName(pkColumn.get(PKTABLE_SCHEM), pkColumn.get(PKTABLE_NAME),
                        pkColumn.get(FKCOLUMN_NAME), pkColumn.get(PKCOLUMN_NAME)));

        if (ctx.on_delete_clause() != null) {
            pkColumn.put(FK_DROP_IS_CASCADE, on_delete_clause(ctx.on_delete_clause()));
        }

        fkColumns.add(pkColumn);

        tableEditor.setForeignKeys(fkColumns);
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

    @Override
    public void enterModify_col_properties(PlSqlParser.Modify_col_propertiesContext ctx) {
        String resolveColumnDataType = resolveColumnDataType(ctx);
        List<String> modifyKeys = new ArrayList<>();
        if (ctx.DEFAULT() != null) {
            columnEditor.defaultValueExpression(ctx.column_default_value().getText());
            modifyKeys.add(DEFAULT_VALUE_KEY);
        }
        if (resolveColumnDataType.equals(OPTIONAL_KEY)) {
            modifyKeys.add(resolveColumnDataType);
        }
        columnEditor.modifyKeys(modifyKeys);
        super.enterModify_col_properties(ctx);
    }

    // todo use dataTypeResolver instead
    private void resolveColumnDataType(PlSqlParser.Column_definitionContext ctx) {
        if (ctx.column_name() == null) {
            return;
        }

        columnEditor.name(getColumnName(ctx.column_name()));

        boolean hasNotNullConstraint = ctx.inline_constraint().stream().anyMatch(c -> c.NOT() != null);
        columnEditor.optional(!hasNotNullConstraint);

        if (ctx.datatype() == null) {
            if (ctx.type_name() != null && "\"MDSYS\".\"SDO_GEOMETRY\"".equalsIgnoreCase(ctx.type_name().getText())) {
                columnEditor.jdbcType(Types.STRUCT).type("MDSYS.SDO_GEOMETRY");
            }
        }
        else {
            resolveColumnDataType(ctx.datatype());
        }
    }

    private String resolveColumnDataType(PlSqlParser.Modify_col_propertiesContext ctx) {
        columnEditor.name(getColumnName(ctx.column_name()));

        if (ctx.datatype() != null) {
            resolveColumnDataType(ctx.datatype());
        }

        boolean hasNullConstraint = ctx.inline_constraint().stream().anyMatch(c -> c.NULL_() != null);
        boolean hasNotNullConstraint = ctx.inline_constraint().stream().anyMatch(c -> c.NOT() != null);
        if (hasNotNullConstraint && columnEditor.isOptional()) {
            columnEditor.optional(false);
            return OPTIONAL_KEY;
        }
        else if (hasNullConstraint && !columnEditor.isOptional()) {
            columnEditor.optional(true);
            return OPTIONAL_KEY;
        }

        return StringUtil.EMPTY_STRING;
    }

    private void resolveColumnDataType(PlSqlParser.DatatypeContext ctx) {
        PlSqlParser.Precision_partContext precisionPart = null;
        if (ctx != null) {
            precisionPart = ctx.precision_part();
        }

        if (ctx != null && ctx.native_datatype_element() != null) {
            if (ctx.native_datatype_element().INT() != null
                    || ctx.native_datatype_element().INTEGER() != null
                    || ctx.native_datatype_element().SMALLINT() != null
                    || ctx.native_datatype_element().NUMERIC() != null
                    || ctx.native_datatype_element().DECIMAL() != null) {
                // NUMERIC and DECIMAL types have by default zero scale
                columnEditor
                        .jdbcType(Types.NUMERIC)
                        .type("NUMBER");

                if (precisionPart == null) {
                    columnEditor.length(38)
                            .scale(0);
                }
                else {
                    setPrecision(precisionPart, columnEditor);
                    setScale(precisionPart, columnEditor);
                }
            }
            else if (ctx.native_datatype_element().DATE() != null) {
                // JDBC driver reports type as timestamp but name DATE
                columnEditor
                        .jdbcType(Types.TIMESTAMP)
                        .type("DATE");
            }
            else if (ctx.native_datatype_element().TIMESTAMP() != null) {
                if (ctx.WITH() != null
                        && ctx.TIME() != null
                        && ctx.ZONE() != null) {
                    if (ctx.LOCAL() != null) {
                        columnEditor
                                .jdbcType(OracleTypes.TIMESTAMPLTZ)
                                .type("TIMESTAMP WITH LOCAL TIME ZONE");
                    }
                    else {
                        columnEditor
                                .jdbcType(OracleTypes.TIMESTAMPTZ)
                                .type("TIMESTAMP WITH TIME ZONE");
                    }
                }
                else {
                    columnEditor
                            .jdbcType(Types.TIMESTAMP)
                            .type("TIMESTAMP");
                }

                if (precisionPart == null) {
                    columnEditor.length(6);
                }
                else {
                    setPrecision(precisionPart, columnEditor);
                }
            }
            // VARCHAR is the same as VARCHAR2 in Oracle
            else if (ctx.native_datatype_element().VARCHAR2() != null ||
                    ctx.native_datatype_element().VARCHAR() != null) {
                columnEditor
                        .jdbcType(Types.VARCHAR)
                        .type("VARCHAR2");

                if (precisionPart == null) {
                    columnEditor.length(getVarCharDefaultLength());
                }
                else {
                    setPrecision(precisionPart, columnEditor);
                }
            }
            else if (ctx.native_datatype_element().NVARCHAR2() != null) {
                columnEditor
                        .jdbcType(Types.NVARCHAR)
                        .type("NVARCHAR2");

                if (precisionPart == null) {
                    columnEditor.length(getVarCharDefaultLength());
                }
                else {
                    setPrecision(precisionPart, columnEditor);
                }
            }
            else if (ctx.native_datatype_element().CHAR() != null) {
                columnEditor
                        .jdbcType(Types.CHAR)
                        .type("CHAR")
                        .length(1);

                if (precisionPart != null) {
                    setPrecision(precisionPart, columnEditor);
                }
            }
            else if (ctx.native_datatype_element().NCHAR() != null) {
                columnEditor
                        .jdbcType(Types.NCHAR)
                        .type("NCHAR")
                        .length(1);

                if (precisionPart != null) {
                    setPrecision(precisionPart, columnEditor);
                }
            }
            else if (ctx.native_datatype_element().BINARY_FLOAT() != null) {
                columnEditor
                        .jdbcType(OracleTypes.BINARY_FLOAT)
                        .type("BINARY_FLOAT");
            }
            else if (ctx.native_datatype_element().BINARY_DOUBLE() != null) {
                columnEditor
                        .jdbcType(OracleTypes.BINARY_DOUBLE)
                        .type("BINARY_DOUBLE");
            }
            // PRECISION keyword is mandatory
            else if (ctx.native_datatype_element().FLOAT() != null ||
                    (ctx.native_datatype_element().DOUBLE() != null && ctx.native_datatype_element().PRECISION() != null)) {
                columnEditor
                        .jdbcType(Types.FLOAT)
                        .type("FLOAT")
                        .length(126);

                // TODO float's precision is about bits not decimal digits; should be ok for now to over-size
                if (precisionPart != null) {
                    setPrecision(precisionPart, columnEditor);
                }
            }
            else if (ctx.native_datatype_element().REAL() != null) {
                columnEditor
                        .jdbcType(Types.FLOAT)
                        .type("FLOAT")
                        // TODO float's precision is about bits not decimal digits; should be ok for now to over-size
                        .length(63);
            }
            else if (ctx.native_datatype_element().NUMBER() != null) {
                columnEditor
                        .jdbcType(Types.NUMERIC)
                        .type("NUMBER");

                if (precisionPart == null) {
                    columnEditor.length(38);
                }
                else {
                    if (precisionPart.ASTERISK() != null) {
                        // when asterisk is used, explicitly set precision to 38
                        columnEditor.length(38);
                    }
                    else {
                        setPrecision(precisionPart, columnEditor);
                    }
                    setScale(precisionPart, columnEditor);
                }
            }
            else if (ctx.native_datatype_element().BLOB() != null) {
                columnEditor
                        .jdbcType(Types.BLOB)
                        .type("BLOB");
            }
            else if (ctx.native_datatype_element().CLOB() != null) {
                columnEditor
                        .jdbcType(Types.CLOB)
                        .type("CLOB");
            }
            else if (ctx.native_datatype_element().RAW() != null) {
                columnEditor
                        .jdbcType(OracleTypes.RAW)
                        .type("RAW");

                setPrecision(precisionPart, columnEditor);
            }
            else if (ctx.native_datatype_element().SDO_GEOMETRY() != null) {
                // Allows the registration of new SDO_GEOMETRY columns via an CREATE/ALTER TABLE
                // This is the same registration of the column that is resolved during JDBC metadata inspection.
                columnEditor
                        .jdbcType(OracleTypes.OTHER)
                        .type("SDO_GEOMETRY")
                        .length(1);
            }
            else if (ctx.native_datatype_element().ROWID() != null) {
                columnEditor
                        .jdbcType(Types.VARCHAR)
                        .type("ROWID");
            }
            else {
                throw new IllegalArgumentException("Unsupported column type: " + ctx.native_datatype_element().getText());
            }
        }
        else if (ctx.INTERVAL() != null
                && ctx.YEAR() != null
                && ctx.TO() != null
                && ctx.MONTH() != null) {
            columnEditor
                    .jdbcType(OracleTypes.INTERVALYM)
                    .type("INTERVAL YEAR TO MONTH")
                    .length(2);
            if (!ctx.expression().isEmpty()) {
                columnEditor.length(Integer.valueOf((ctx.expression(0).getText())));
            }
        }
        else if (ctx.INTERVAL() != null
                && ctx.DAY() != null
                && ctx.TO() != null
                && ctx.SECOND() != null) {
            columnEditor
                    .jdbcType(OracleTypes.INTERVALDS)
                    .type("INTERVAL DAY TO SECOND")
                    .length(2)
                    .scale(6);
            for (final PlSqlParser.ExpressionContext e : ctx.expression()) {
                if (e.getSourceInterval().startsAfter(ctx.TO().getSourceInterval())) {
                    columnEditor.scale(Integer.valueOf(e.getText()));
                }
                else {
                    columnEditor.length(Integer.valueOf(e.getText()));
                }
            }
            if (!ctx.expression().isEmpty()) {
                columnEditor.length(Integer.valueOf((ctx.expression(0).getText())));
            }
        }
        else {
            throw new IllegalArgumentException("Unsupported column type: " + ctx.getText());
        }
    }

    private int getVarCharDefaultLength() {
        // TODO replace with value from select name, value from v$parameter where name='max_string_size';
        return 4000;
    }

    private void setPrecision(PlSqlParser.Precision_partContext precisionPart, ColumnEditor columnEditor) {
        columnEditor.length(Integer.valueOf(precisionPart.numeric(0).getText()));
    }

    private void setScale(PlSqlParser.Precision_partContext precisionPart, ColumnEditor columnEditor) {
        if (precisionPart.numeric().size() > 1) {
            columnEditor.scale(Integer.valueOf(precisionPart.numeric(1).getText()));
        }
        else if (precisionPart.numeric_negative() != null) {
            columnEditor.scale(Integer.valueOf(precisionPart.numeric_negative().getText()));
        }
        else {
            columnEditor.scale(0);
        }
    }
}
