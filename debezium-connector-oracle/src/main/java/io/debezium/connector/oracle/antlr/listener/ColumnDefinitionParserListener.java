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

import org.antlr.v4.runtime.tree.ParseTreeListener;

import io.debezium.antlr.DataTypeResolver;
import io.debezium.connector.oracle.antlr.OracleDdlParser;
import io.debezium.ddl.parser.oracle.generated.PlSqlParser;
import io.debezium.ddl.parser.oracle.generated.PlSqlParser.Column_nameContext;
import io.debezium.ddl.parser.oracle.generated.PlSqlParser.Foreign_key_clauseContext;
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
 * Modified by an in 2020.5.30 for foreign key feature
 */
public class ColumnDefinitionParserListener extends BaseParserListener {

    private final OracleDdlParser parser;
    private final DataTypeResolver dataTypeResolver;
    private final TableEditor tableEditor;
    private final List<ParseTreeListener> listeners;
    private ColumnEditor columnEditor;

    public static final String PKTABLE_SCHEM = "pktableSchem";
    public static final String PKTABLE_NAME = "pktableName";
    public static final String PKCOLUMN_NAME = "pkColumnName";
    public static final String FKCOLUMN_NAME = "fkColumnName";
    public static final String FK_NAME = "fkName";

    public static final String DOT = "\\.";

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
        Foreign_key_clauseContext foreign_key_clause = ctx.foreign_key_clause();

        if (foreign_key_clause != null) {
            enterForeign_key_clause(foreign_key_clause, ctx.constraint_name().getText());
        }

        super.enterOut_of_line_constraint(ctx);
    }

    public void enterForeign_key_clause(PlSqlParser.Foreign_key_clauseContext ctx, String fkName) {

        List<Map<String, String>> fkColumns = new ArrayList<Map<String, String>>();

        final Map<String, String> pkColumn = new HashMap<>();

        List<Column_nameContext> parens = ctx.paren_column_list().column_list().column_name();

        List<Column_nameContext> references = ctx.references_clause().paren_column_list()
                .column_list().column_name();
        
        String[] tableId = ctx.references_clause().tableview_name().getText().split(DOT);
        
        pkColumn.put(PKTABLE_SCHEM, getForeignAttribute(tableId.length > 1 ? tableId[0] : StringUtil.EMPTY_STRING));
        pkColumn.put(PKTABLE_NAME, getForeignAttribute(tableId.length > 1 ? tableId[1] : tableId[0]));
        pkColumn.put(FK_NAME, getForeignAttribute(fkName));

        for (int i = 0; i < references.size(); i++) {
            pkColumn.put(PKCOLUMN_NAME, getForeignAttribute(references.get(i).getText()));
            pkColumn.put(FKCOLUMN_NAME, getForeignAttribute(parens.get(i).getText()));
            fkColumns.add(pkColumn);
        }

        tableEditor.setForeignKeys(fkColumns);
    }

    @Override
    public void enterModify_col_properties(PlSqlParser.Modify_col_propertiesContext ctx) {
        resolveColumnDataType(ctx);
        if (ctx.DEFAULT() != null) {
            columnEditor.defaultValueExpression(ctx.column_default_value().getText());
        }
        super.enterModify_col_properties(ctx);
    }

    // todo use dataTypeResolver instead
    private void resolveColumnDataType(PlSqlParser.Column_definitionContext ctx) {
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

    private void resolveColumnDataType(PlSqlParser.Modify_col_propertiesContext ctx) {
        columnEditor.name(getColumnName(ctx.column_name()));

        resolveColumnDataType(ctx.datatype());

        boolean hasNullConstraint = ctx.inline_constraint().stream().anyMatch(c -> c.NULL_() != null);
        boolean hasNotNullConstraint = ctx.inline_constraint().stream().anyMatch(c -> c.NOT() != null);
        if (hasNotNullConstraint && columnEditor.isOptional()) {
            columnEditor.optional(false);
        }
        else if (hasNullConstraint && !columnEditor.isOptional()) {
            columnEditor.optional(true);
        }
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
