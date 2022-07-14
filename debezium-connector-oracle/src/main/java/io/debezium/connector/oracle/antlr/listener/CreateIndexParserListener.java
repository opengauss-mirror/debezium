/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.antlr.listener;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.oracle.antlr.OracleDdlParser;
import io.debezium.ddl.parser.oracle.generated.PlSqlParser;
import io.debezium.relational.Index;
import io.debezium.relational.TableEditor;
import io.debezium.relational.TableId;

/**
 * @author saxisuer
 */
public class CreateIndexParserListener extends BaseParserListener {
    private static final Logger LOGGER = LoggerFactory.getLogger(CreateIndexParserListener.class);

    private TableEditor tableEditor;
    private String catalogName;
    private String schemaName;
    private OracleDdlParser parser;
    private String indexName;
    private Index index;

    CreateIndexParserListener(final String catalogName, final String schemaName, final OracleDdlParser parser) {
        this.catalogName = catalogName;
        this.schemaName = schemaName;
        this.parser = parser;
    }

    /**
     * enter create index
     *
     * @param ctx the parse tree
     */
    @Override
    public void enterCreate_index(PlSqlParser.Create_indexContext ctx) {
        index = null;
        if (ctx.index_name().id_expression() == null) {
            indexName = getTableOrColumnName(ctx.index_name().identifier().getText());
        }
        else {
            indexName = getTableOrColumnName(ctx.index_name().id_expression().getText());
        }
        String tableName = getTableName(ctx.table_index_clause().tableview_name());
        TableId tableId = new TableId(catalogName, schemaName, tableName);
        if (parser.databaseTables().forTable(tableId) == null) {
            LOGGER.info("Ignoring CREATE INDEX statement for non-captured table {}", tableId);
            return;
        }
        tableEditor = parser.databaseTables().editTable(tableId);
        tableEditor.clearColumnChange();
        tableEditor.clearConstraint();
        LOGGER.info("PARSING CREATE INDEX, [{}.{}] FOR TABLE: [{}]", schemaName, indexName, tableId.identifier());
        index = new Index();
        index.setIndexName(indexName);
        index.setTableId(tableId.identifier());
        index.setTableName(tableName);
        index.setSchemaName(schemaName);
        if (ctx.UNIQUE() != null) {
            index.setUnique(true);
        }
        super.enterCreate_index(ctx);
    }

    @Override
    public void exitCreate_index(PlSqlParser.Create_indexContext ctx) {
        parser.runIfNotNull(() -> {
            tableEditor.setChangeIndex(index).addIndex(indexName);
            parser.databaseTables().overwriteTable(tableEditor.create());
            parser.databaseTables().bindTableIndex(indexName, tableEditor.tableId());
            parser.signalCreateIndex(indexName, tableEditor.tableId(), ctx);
        }, tableEditor, index);
        super.exitCreate_index(ctx);
    }

    @Override
    public void enterTable_index_clause(PlSqlParser.Table_index_clauseContext ctx) {
        parser.runIfNotNull(() -> {
            List<PlSqlParser.Index_column_exprContext> indexColumnExprContexts = ctx.index_column_expr();
            for (PlSqlParser.Index_column_exprContext indexColumnExprContext : indexColumnExprContexts) {
                PlSqlParser.Index_exprContext indexExprContext = indexColumnExprContext.index_expr();
                Index.IndexColumnExpr indexColumnExpr = new Index.IndexColumnExpr();
                if (indexExprContext.expression() != null) {
                    PlSqlParser.ExpressionContext expression = indexExprContext.expression();
                    List<PlSqlParser.ConcatenationContext> concatenation = expression.logical_expression()
                            .unary_logical_expression()
                            .multiset_expression()
                            .relational_expression()
                            .compound_expression()
                            .concatenation();
                    for (PlSqlParser.ConcatenationContext concatenationContext : concatenation) {
                        indexColumnExpr.setDesc(indexColumnExprContext.DESC() != null);
                        List<String> includeColumn = new ArrayList<>();
                        analyzeConcatenationContext(concatenationContext, includeColumn);
                        String rawExpr = concatenationContext.getText();
                        for (int i = 0; i < includeColumn.size(); i++) {
                            rawExpr = rawExpr.replace(includeColumn.get(i), ":$" + i);
                        }
                        includeColumn = includeColumn.stream().map(BaseParserListener::getTableOrColumnName).collect(Collectors.toList());
                        indexColumnExpr.setColumnExpr(rawExpr);
                        indexColumnExpr.setIncludeColumn(includeColumn);
                        LOGGER.debug("{} {},INCLUDE_COLUMN:{}", rawExpr, indexColumnExprContext.DESC() != null ? "DESC" : "ASC", includeColumn);
                    }
                }
                else {
                    indexColumnExpr.setColumnExpr(":$0");
                    indexColumnExpr.setDesc(indexColumnExprContext.DESC() != null);
                    indexColumnExpr.setIncludeColumn(Collections.singletonList(getTableOrColumnName(indexExprContext.column_name()
                            .identifier()
                            .id_expression()
                            .getText())));

                    LOGGER.debug("column name: {}, sort: {}",
                            indexExprContext.column_name().identifier().id_expression().getText(),
                            indexColumnExprContext.DESC() != null ? "DESC" : "ASC");
                }
                index.getIndexColumnExpr().add(indexColumnExpr);
            }
        }, tableEditor, index);
        super.enterTable_index_clause(ctx);
    }

    private void analyzeConcatenationContext(PlSqlParser.ConcatenationContext concatenationContext, List<String> includeColumn) {
        if (concatenationContext.concatenation().size() > 0) {
            for (PlSqlParser.ConcatenationContext context : concatenationContext.concatenation()) {
                analyzeConcatenationContext(context, includeColumn);
            }
        }
        else {
            PlSqlParser.Unary_expressionContext unaryExpressionContext = concatenationContext.model_expression().unary_expression();
            PlSqlParser.Standard_functionContext standardFunctionContext = unaryExpressionContext.standard_function();
            PlSqlParser.AtomContext atom = unaryExpressionContext.atom();
            if (null != standardFunctionContext) {
                analyzeStandardFunction(standardFunctionContext, includeColumn);
            }
            if (null != atom) {
                if (atom.bind_variable() != null) {
                    LOGGER.debug("get bind segment {}", atom.bind_variable().getText());
                }
                else {
                    PlSqlParser.ConstantContext constant = atom.constant();
                    PlSqlParser.General_elementContext generalElementContext = atom.general_element();
                    if (constant != null) {
                        if (constant.NULL_() != null) {
                            LOGGER.debug("get NULL segment {}", constant.getText());
                        }
                        else {
                            List<PlSqlParser.Quoted_stringContext> quotedStringContexts = constant.quoted_string();
                            if (quotedStringContexts.size() > 0) {
                                List<String> collect = quotedStringContexts.stream()
                                        .filter(quoted_stringContext -> quoted_stringContext.variable_name() != null)
                                        .map(quoted_stringContext -> quoted_stringContext.variable_name()
                                                .getText())
                                        .collect(Collectors.toList());
                                includeColumn.addAll(collect);
                            }
                        }
                    }
                    if (generalElementContext != null) {
                        // find function argument segment
                        Optional<PlSqlParser.General_element_partContext> functionArgument = generalElementContext.general_element_part()
                                .stream()
                                .filter(general_element_partContext -> general_element_partContext.function_argument() != null)
                                .findAny();

                        // find argument segment
                        functionArgument.ifPresent(general_element_partContext -> general_element_partContext.function_argument()
                                .argument()
                                .stream()
                                .flatMap(argumentContext -> argumentContext.expression()
                                        .logical_expression()
                                        .unary_logical_expression()
                                        .multiset_expression()
                                        .relational_expression()
                                        .compound_expression()
                                        .concatenation()
                                        .stream())
                                .forEach(each -> {
                                    analyzeConcatenationContext(each,
                                            includeColumn);
                                }));
                    }
                }
            }
        }
    }

    /**
     * analyze standard function : to_char
     *
     * @param string_functionContext
     * @param includeColumn
     */
    private void analyzeStandardStringFunctionToChar(PlSqlParser.String_functionContext string_functionContext, List<String> includeColumn) {
        PlSqlParser.Standard_functionContext standardFunctionContext = string_functionContext.standard_function();
        if (standardFunctionContext != null) {
            analyzeStandardFunction(standardFunctionContext, includeColumn);
        }
        else {
            PlSqlParser.Table_elementContext tableElementContext = string_functionContext.table_element();
            String columnName = tableElementContext.getText();
            List<PlSqlParser.Quoted_stringContext> quoted_stringContexts = string_functionContext.quoted_string();
            includeColumn.add(columnName);
        }
    }

    /**
     * analyze standard string function
     *
     * @param stringFunctionContext
     * @param includeColumn
     */
    private void analyzeStandardStringFunction(PlSqlParser.String_functionContext stringFunctionContext, List<String> includeColumn) {
        // 先判断是否内嵌函数
        if (stringFunctionContext.standard_function() != null) {
            analyzeStandardFunction(stringFunctionContext.standard_function(), includeColumn);
        }
        else {
            if (stringFunctionContext.TO_DATE() != null) {
                analyzeStandardStringFunctionToDate(stringFunctionContext, includeColumn);
            }
            else if (stringFunctionContext.TO_CHAR() != null) {
                analyzeStandardStringFunctionToChar(stringFunctionContext, includeColumn);
            }
            else if (stringFunctionContext.NVL() != null) {
                analyzeStandardStringFunctionNvl(stringFunctionContext, includeColumn);
            }
            else if (stringFunctionContext.SUBSTR() != null) {
                analyzeStandardStringFunctionSubStr(stringFunctionContext, includeColumn);
            }
            else if (stringFunctionContext.DECODE() != null) {
                analyzeStandardStringFunctionDecode(stringFunctionContext, includeColumn);
            }
            else if (stringFunctionContext.TRIM() != null) {
                analyzeStandardStringFunctionTrim(stringFunctionContext, includeColumn);
            }
            else if (stringFunctionContext.CHR() != null) {
                analyzeStandardStringFunctionChar(stringFunctionContext, includeColumn);
            }
        }
    }

    private void analyzeStandardStringFunctionNvl(PlSqlParser.String_functionContext stringFunctionContext, List<String> includeColumn) {
        defaultAnalyzeStandardStringFunction(stringFunctionContext, includeColumn);
    }

    private void analyzeStandardStringFunctionTrim(PlSqlParser.String_functionContext stringFunctionContext, List<String> includeColumn) {
        analyzeConcatenationContext(stringFunctionContext.concatenation(), includeColumn);
    }

    private void analyzeStandardStringFunctionChar(PlSqlParser.String_functionContext stringFunctionContext, List<String> includeColumn) {
        analyzeConcatenationContext(stringFunctionContext.concatenation(), includeColumn);
    }

    private void analyzeStandardStringFunctionDecode(PlSqlParser.String_functionContext stringFunctionContext, List<String> includeColumn) {
        defaultAnalyzeStandardStringFunction(stringFunctionContext, includeColumn);
    }

    private void defaultAnalyzeStandardStringFunction(PlSqlParser.String_functionContext stringFunctionContext, List<String> includeColumn) {
        stringFunctionContext.expression()
                .stream()
                .flatMap(each -> each.logical_expression()
                        .unary_logical_expression()
                        .multiset_expression()
                        .relational_expression()
                        .compound_expression()
                        .concatenation()
                        .stream())
                .forEach(each -> {
                    analyzeConcatenationContext(each, includeColumn);
                });
    }

    private void analyzeStandardStringFunctionSubStr(PlSqlParser.String_functionContext stringFunctionContext, List<String> includeColumn) {
        defaultAnalyzeStandardStringFunction(stringFunctionContext, includeColumn);
    }

    /**
     * analyze standard function to_date
     *
     * @param stringFunctionContext
     */
    private void analyzeStandardStringFunctionToDate(PlSqlParser.String_functionContext stringFunctionContext, List<String> includeColumn) {
        PlSqlParser.Standard_functionContext standardFunctionContext = stringFunctionContext.standard_function();
        if (standardFunctionContext != null) {
            analyzeStandardFunction(standardFunctionContext, includeColumn);
        }
        else {
            for (PlSqlParser.ExpressionContext expressionContext : stringFunctionContext.expression()) {
                expressionContext.logical_expression()
                        .unary_logical_expression()
                        .multiset_expression()
                        .relational_expression()
                        .compound_expression()
                        .concatenation()
                        .forEach(each -> {
                            analyzeConcatenationContext(each, includeColumn);
                        });
            }
        }
    }

    /**
     * analyze standard function
     *
     * @param standardFunctionContext
     * @param includeColumn
     */
    private void analyzeStandardFunction(PlSqlParser.Standard_functionContext standardFunctionContext, List<String> includeColumn) {
        if (standardFunctionContext.string_function() != null) {
            analyzeStandardStringFunction(standardFunctionContext.string_function(), includeColumn);
        }
    }
}
