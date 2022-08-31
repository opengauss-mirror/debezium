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
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.antlr.v4.runtime.tree.TerminalNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.ddl.parser.oracle.generated.PlSqlParser;
import io.debezium.ddl.parser.oracle.generated.PlSqlParser.AtomContext;
import io.debezium.ddl.parser.oracle.generated.PlSqlParser.Column_nameContext;
import io.debezium.ddl.parser.oracle.generated.PlSqlParser.Compound_expressionContext;
import io.debezium.ddl.parser.oracle.generated.PlSqlParser.ConcatenationContext;
import io.debezium.ddl.parser.oracle.generated.PlSqlParser.Constraint_nameContext;
import io.debezium.ddl.parser.oracle.generated.PlSqlParser.ExpressionsContext;
import io.debezium.ddl.parser.oracle.generated.PlSqlParser.Logical_expressionContext;
import io.debezium.ddl.parser.oracle.generated.PlSqlParser.Model_expressionContext;
import io.debezium.ddl.parser.oracle.generated.PlSqlParser.Multiset_expressionContext;
import io.debezium.ddl.parser.oracle.generated.PlSqlParser.Relational_expressionContext;
import io.debezium.ddl.parser.oracle.generated.PlSqlParser.Unary_expressionContext;
import io.debezium.ddl.parser.oracle.generated.PlSqlParser.Unary_logical_expressionContext;
import io.debezium.ddl.parser.oracle.generated.PlSqlParserBaseListener;
import io.netty.util.internal.StringUtil;

/**
 * This class contains common methods for all listeners
 * Modified by an in 2020.7.2 for constraint feature
 */
class BaseParserListener extends PlSqlParserBaseListener {

    protected final Logger LOGGER = LoggerFactory.getLogger(getClass().getName());

    public static final String SPACE = " ";
    public static final char QUO = '\"';
    public static final String MODIFY_COLUMN = "modifyColumn";
    public static final String ADD_COLUMN = "addColumn";
    public static final String DROP_COLUMN = "dropColumn";

    protected static final String UNIQUE_NAME = "unique";

    protected static final String PKTABLE_SCHEM = "pktableSchem";
    protected static final String PKTABLE_NAME = "pktableName";
    protected static final String PKCOLUMN_NAME = "pkColumnName";
    protected static final String FKCOLUMN_NAME = "fkColumnName";
    protected static final String FK_NAME = "fkName";
    protected static final String FK_DROP_IS_CASCADE = "cascade";

    protected static final String INDEX_NAME = "indexName";
    protected static final String USING_INDEX = "usingIndex";
    protected static final String COLUMN_NAME = "columnName";
    protected static final String TYPE_NAME = "type";

    protected static final String CONSTRAINT_NAME = "constraintName";
    protected static final String PRIMARY_KEY_ACTION = "action";
    protected static final String PRIMARY_KEY_ADD = "add";

    protected static final String CONDITION = "condition";
    protected static final String INCLUDE_COLUMN = "includeColumn";

    protected static final String DEFAULT_VALUE_KEY = "defaultValueExpression";
    protected static final String OPTIONAL_KEY = "optional";

    protected static final String DOT = "\\.";

    protected static final String PRIMARY_DROP_IS_CASCADE = "cascade";

    String getTableName(final PlSqlParser.Tableview_nameContext tableview_name) {
        final String tableName;
        if (tableview_name.id_expression() != null) {
            tableName = tableview_name.id_expression().getText();
        }
        else {
            tableName = tableview_name.identifier().id_expression().getText();
        }
        return getTableOrColumnName(tableName);
    }

    String getTableName(final PlSqlParser.Column_nameContext ctx) {
        final String tableName;
        if (ctx.id_expression() != null && ctx.id_expression().size() > 1) {
            tableName = getTableOrColumnName(ctx.id_expression(0).getText());
        }
        else {
            tableName = getTableOrColumnName(ctx.identifier().id_expression().getText());
        }
        return tableName;
    }

    String getColumnName(final PlSqlParser.Column_nameContext ctx) {
        final String columnName;
        if (ctx.id_expression() != null && ctx.id_expression().size() > 0) {
            columnName = getTableOrColumnName(ctx.id_expression(ctx.id_expression().size() - 1).getText());
        }
        else {
            columnName = getTableOrColumnName(ctx.identifier().id_expression().getText());
        }
        return columnName;
    }

    String getAttribute(final String attribute) {
        return removeQuotes(attribute, true);
    }

    String getColumnName(final PlSqlParser.Old_column_nameContext ctx) {
        return getTableOrColumnName(ctx.getText());
    }

    String getColumnName(final PlSqlParser.New_column_nameContext ctx) {
        return getTableOrColumnName(ctx.getText());
    }

    /**
     * Resolves a table or column name from the provided string.
     *
     * Oracle table and column names are inherently stored in upper-case; however, if the objects
     * are created using double-quotes, the case of the object name is retained.  Therefore when
     * needing to parse a table or column name, this method will adhere to those rules and will
     * always return the name in upper-case unless the provided name is double-quoted in which
     * the returned value will have the double-quotes removed and case retained.
     *
     * @param name table or column name
     * @return parsed table or column name from the supplied name argument
     */
    protected static String getTableOrColumnName(String name) {
        return removeQuotes(name, true);
    }

    /**
     * Removes leading and trailing double quote characters from the provided string.
     *
     * @param text value to have double quotes removed
     * @param upperCaseIfNotQuoted control if returned string is upper-cased if not quoted
     * @return string that has had quotes removed
     */
    @SuppressWarnings("SameParameterValue")
    private static String removeQuotes(String text, boolean upperCaseIfNotQuoted) {
        if (text != null && text.length() > 2 && text.startsWith("\"") && text.endsWith("\"")) {
            return text.substring(1, text.length() - 1);
        }
        return upperCaseIfNotQuoted ? text.toUpperCase() : text;
    }

    protected List<String> Logical_expression_parse(Logical_expressionContext logical_expressionContext) {
        List<String> includeColumn = new ArrayList<>();
        Logical_expression_parse(logical_expressionContext, includeColumn);
        return includeColumn.stream().distinct().collect(Collectors.toList());
    }

    private void Logical_expression_parse(Logical_expressionContext logical_expression, List<String> includeColumn) {
        Optional<TerminalNode> terminalNode = Logical_expression_node(logical_expression);
        if (terminalNode.isPresent()) {
            logical_expression.logical_expression().forEach(each -> {
                Logical_expression_parse(each, includeColumn);
            });
        }
        else {
            unary_logical_expression_parse(logical_expression.unary_logical_expression(), includeColumn);
        }
    }

    private Optional<TerminalNode> Logical_expression_node(Logical_expressionContext logical_expression) {
        return Stream.of(
                logical_expression.AND(),
                logical_expression.OR()).filter(Objects::nonNull).findFirst();
    }

    private List<String> unary_logical_expression_parse(Unary_logical_expressionContext unary_logical_expression, List<String> includeColumn) {

        if (unary_logical_expression == null) {
            return includeColumn;
        }

        Multiset_expressionContext multiset_expression = unary_logical_expression.multiset_expression();
        Relational_expressionContext relational_expression = multiset_expression.relational_expression();

        multisetExpression(relational_expression.compound_expression(), includeColumn);
        List<Relational_expressionContext> relational_expressions = relational_expression.relational_expression();
        for (Relational_expressionContext relationalExpression : relational_expressions) {
            multisetExpression(relationalExpression.compound_expression(), includeColumn);
        }
        return includeColumn;
    }

    private List<String> multisetExpression(Compound_expressionContext compound_expression, List<String> includeColumn) {
        if (compound_expression == null) {
            return includeColumn;
        }
        compound_expression.concatenation().forEach(concatenation -> concatenation(concatenation, includeColumn));

        return includeColumn;
    }

    private void concatenation(ConcatenationContext ctx, List<String> includeColumn) {

        Model_expressionContext model_expression = ctx.model_expression();

        if (model_expression != null) {
            Unary_expressionContext unary_expression = model_expression.unary_expression();
            unary_expression(unary_expression, includeColumn);
        }

        List<ConcatenationContext> concatenations = ctx.concatenation();
        if (concatenations != null && concatenations.size() > 0) {
            concatenations.forEach(concatenation -> concatenation(concatenation, includeColumn));
        }
    }

    private void unary_expression(Unary_expressionContext ctx, List<String> includeColumn) {

        Unary_expressionContext unary_expression = ctx.unary_expression();
        if (unary_expression != null) {
            unary_expression(unary_expression, includeColumn);
        }
        AtomContext atom = ctx.atom();
        if (atom != null) {
            PlSqlParser.ConstantContext constant = atom.constant();
            if (constant != null) {
                List<PlSqlParser.Quoted_stringContext> quotedStringContexts = constant.quoted_string();
                if (quotedStringContexts.size() > 0) {
                    quotedStringContexts.stream()
                            .filter(quoted_stringContext -> quoted_stringContext.variable_name() != null)
                            .map(quoted_stringContext -> quoted_stringContext.variable_name().getText())
                            .forEach(includeColumn::add);
                }
            }
            PlSqlParser.General_elementContext general_elementContext = atom.general_element();
            if (general_elementContext != null) {
                general_elementContext.general_element_part()
                        .stream()
                        .filter(each -> each.function_argument() != null)
                        .flatMap(each -> each.function_argument().argument().stream())
                        .forEach(each -> Logical_expression_parse(each.expression().logical_expression(), includeColumn));
            }

            ExpressionsContext expressions = atom.expressions();
            if (expressions != null && expressions.expression() != null) {
                expressions.expression().forEach(expression -> Logical_expression_parse(expression.logical_expression(), includeColumn));
            }

        }

    }

    protected List<Map<String, String>> enterInline_ref_constraint(PlSqlParser.References_clauseContext ctx, String schema, String columnName,
                                                                   Constraint_nameContext constraint_name) {

        List<Map<String, String>> fkColumns = new ArrayList<Map<String, String>>();

        final Map<String, String> pkColumn = new HashMap<>();

        List<Column_nameContext> references = ctx.paren_column_list()
                .column_list().column_name();

        String[] tableId = ctx.tableview_name().getText().split(DOT);

        pkColumn.put(PKTABLE_SCHEM, getAttribute(tableId.length > 1 ? tableId[0] : schema));
        pkColumn.put(PKTABLE_NAME, getAttribute(tableId.length > 1 ? tableId[1] : tableId[0]));

        for (int i = 0; i < references.size(); i++) {
            pkColumn.put(PKCOLUMN_NAME, getAttribute(references.get(i).getText()));
            pkColumn.put(FKCOLUMN_NAME, getAttribute(columnName));
            fkColumns.add(pkColumn);
        }

        pkColumn.put(FK_NAME, constraint_name == null ? buildInlineFkName(pkColumn.get(PKTABLE_SCHEM),
                pkColumn.get(PKTABLE_NAME), columnName, pkColumn.get(PKCOLUMN_NAME)) : getTableOrColumnName(constraint_name.getText()));
        return fkColumns;
    }

    protected String buildInlineFkName(String pkScheme, String pkTableName, String pkColumnName, String fkColumnName) {
        StringBuilder sb = new StringBuilder();
        sb.append("SYS_FK_");
        sb.append(pkScheme == null ? StringUtil.EMPTY_STRING : pkScheme.replaceAll("#", "_"));
        sb.append("_").append(pkTableName);
        sb.append("_").append(pkColumnName.replaceAll(String.valueOf(StringUtil.COMMA), "_"));
        sb.append("_").append(fkColumnName.replaceAll(String.valueOf(StringUtil.COMMA), "_"));
        return sb.toString();
    }

    protected String getUsingIndexName(PlSqlParser.Constraint_stateContext constraint_stateContext) {
        if (constraint_stateContext != null) {
            List<PlSqlParser.Using_index_clauseContext> using_index_clauseContexts = constraint_stateContext.using_index_clause();
            for (PlSqlParser.Using_index_clauseContext using_index_clauseContext : using_index_clauseContexts) {
                PlSqlParser.Index_nameContext index_nameContext = using_index_clauseContext.index_name();
                if (index_nameContext != null) {
                    String indexName;
                    if (index_nameContext.id_expression() == null) {
                        indexName = getTableOrColumnName(index_nameContext.identifier().getText());
                    }
                    else {
                        indexName = getTableOrColumnName(index_nameContext.id_expression().getText());
                    }
                    LOGGER.info("Constraint using index: {}", indexName);
                    return indexName;
                }
            }
        }
        return null;
    }
}
