/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.antlr.listener;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.antlr.v4.runtime.tree.TerminalNode;

import io.debezium.ddl.parser.oracle.generated.PlSqlParser;
import io.debezium.ddl.parser.oracle.generated.PlSqlParser.AtomContext;
import io.debezium.ddl.parser.oracle.generated.PlSqlParser.Between_elementsContext;
import io.debezium.ddl.parser.oracle.generated.PlSqlParser.Compound_expressionContext;
import io.debezium.ddl.parser.oracle.generated.PlSqlParser.ConcatenationContext;
import io.debezium.ddl.parser.oracle.generated.PlSqlParser.ExpressionsContext;
import io.debezium.ddl.parser.oracle.generated.PlSqlParser.In_elementsContext;
import io.debezium.ddl.parser.oracle.generated.PlSqlParser.Logical_expressionContext;
import io.debezium.ddl.parser.oracle.generated.PlSqlParser.Logical_operationContext;
import io.debezium.ddl.parser.oracle.generated.PlSqlParser.Model_expressionContext;
import io.debezium.ddl.parser.oracle.generated.PlSqlParser.Multiset_expressionContext;
import io.debezium.ddl.parser.oracle.generated.PlSqlParser.Relational_expressionContext;
import io.debezium.ddl.parser.oracle.generated.PlSqlParser.Relational_operatorContext;
import io.debezium.ddl.parser.oracle.generated.PlSqlParser.Type_specContext;
import io.debezium.ddl.parser.oracle.generated.PlSqlParser.Unary_expressionContext;
import io.debezium.ddl.parser.oracle.generated.PlSqlParser.Unary_logical_expressionContext;
import io.debezium.ddl.parser.oracle.generated.PlSqlParserBaseListener;
import io.netty.util.internal.StringUtil;

/**
 * This class contains common methods for all listeners
 * Modified by an in 2020.7.2 for constraint feature
 */
class BaseParserListener extends PlSqlParserBaseListener {

    public static final String SPACE = " ";
    public static final char QUO = '\"';

    public static Boolean IS_COLUMN = true;

    protected static final String UNIQUE_NAME = "unique";

    protected static final String PKTABLE_SCHEM = "pktableSchem";
    protected static final String PKTABLE_NAME = "pktableName";
    protected static final String PKCOLUMN_NAME = "pkColumnName";
    protected static final String FKCOLUMN_NAME = "fkColumnName";
    protected static final String FK_NAME = "fkName";
    protected static final String FK_DROP_IS_CASCADE = "cascade";

    protected static final String INDEX_NAME = "indexName";
    protected static final String COLUMN_NAME = "columnName";
    protected static final String TYPE_NAME = "type";

    protected static final String CONSTRAINT_NAME = "constraintName";
    protected static final String PRIMARY_KEY_ACTION = "action";
    protected static final String PRIMARY_KEY_ADD = "add";

    protected static final String CONDITION = "condition";

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
        return removeQuotes(attribute, false);
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
    private static String getTableOrColumnName(String name) {
        return removeQuotes(name, false);
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

    public String addSpace(Object str) {
        StringBuilder sb = new StringBuilder();
        sb.append(SPACE)
                .append(str)
                .append(SPACE);
        return sb.toString();
    }

    public String addLeftSpace(Object str) {
        StringBuilder sb = new StringBuilder();
        sb.append(SPACE)
                .append(str);
        return sb.toString();
    }

    public String addQuo(Object str) {
        if (str != null && str.toString().contains("\"")) {
            return str.toString();
        }
        StringBuilder sb = new StringBuilder();
        sb.append(QUO)
                .append(str)
                .append(QUO);
        return sb.toString();
    }

    protected String Logical_expression_parse(Logical_expressionContext logical_expression) {

        StringBuilder sb = new StringBuilder();

        sb.append(unary_logical_expression_parse(logical_expression.unary_logical_expression()));

        Logical_expression_node(logical_expression).ifPresent(node -> {
            List<String> logical_expressions = logical_expression.logical_expression().stream().map(logical_exp -> Logical_expression_parse(logical_exp))
                    .collect(Collectors.toList());
            sb.append(StringUtil.join(addSpace(node.getText()), logical_expressions));
        });

        return sb.toString();
    }

    private Optional<TerminalNode> Logical_expression_node(Logical_expressionContext logical_expression) {
        return Arrays.asList(
                logical_expression.AND(),
                logical_expression.OR())
                .stream().filter(node -> node != null).findFirst();
    }

    private String unary_logical_expression_parse(Unary_logical_expressionContext unary_logical_expression) {
        StringBuilder sb = new StringBuilder();
        if (unary_logical_expression == null) {
            return sb.toString();
        }

        IS_COLUMN = true;

        Multiset_expressionContext multiset_expression = unary_logical_expression.multiset_expression();
        Relational_expressionContext relational_expression = multiset_expression.relational_expression();

        sb.append(multiset_expression(relational_expression.compound_expression()));

        List<Relational_expressionContext> relational_expressions = relational_expression.relational_expression();
        Relational_operatorContext relational_operator = relational_expression.relational_operator();
        if (relational_operator != null) {
            List<String> exps = relational_expressions.stream().map(exp -> multiset_expression(exp.compound_expression())).collect(Collectors.toList());
            sb.append(StringUtil.join(addSpace(relational_operator.getText()), exps));
        }

        List<TerminalNode> unary_logical_expression_nodes = unary_logical_expression_node(unary_logical_expression);
        if (unary_logical_expression_nodes.size() <= 0) {
            return sb.toString();
        }

        unary_logical_expression_nodes.forEach(node -> sb.append(addLeftSpace(node.getText())));
        sb.append(logical_operation_expression(unary_logical_expression));

        return sb.toString();
    }

    private String multiset_expression(Compound_expressionContext compound_expression) {
        StringBuilder sb = new StringBuilder();
        if (compound_expression == null) {
            return sb.toString();
        }

        List<String> concatenations = compound_expression.concatenation().stream().map(concatenation -> {
            String concatenationStr = IS_COLUMN ? addQuo(concatenation(concatenation)) : concatenation(concatenation);
            IS_COLUMN = false;
            return concatenationStr;
        }).collect(Collectors.toList());

        Optional<TerminalNode> multiset_expression_node_middle = multiset_expression_node_middle(compound_expression);
        if (multiset_expression_node_middle.isPresent()) {
            if (concatenations.size() == 1) {
                sb.append(concatenations.get(0)).append(addSpace(multiset_expression_node_middle.get().getText()));
            }
            else {
                sb.append(StringUtil.join(addSpace(multiset_expression_node_middle.get().getText()), concatenations));
            }
        }
        else {
            sb.append(StringUtil.join(SPACE, concatenations));
        }

        multiset_expression_node(compound_expression).ifPresent(node -> {
            sb.append(addSpace(node.getText()));
            switch (node.getText().toUpperCase()) {
                case "IN":
                    sb.append(in_elements(compound_expression.in_elements()));
                    break;
                case "BETWEEN":
                    sb.append(between_elements(compound_expression.between_elements()));
                    break;
                default:
                    break;
            }
        });

        return sb.toString();
    }

    private String concatenation(ConcatenationContext ctx) {
        StringBuilder sb = new StringBuilder();

        Model_expressionContext model_expression = ctx.model_expression();

        if (model_expression != null) {
            Unary_expressionContext unary_expression = model_expression.unary_expression();
            sb.append(unary_expression(unary_expression));
        }

        List<ConcatenationContext> concatenations = ctx.concatenation();
        if (concatenations != null && concatenations.size() > 0) {
            List<String> concatenationList = concatenations.stream().map(concatenation -> concatenation(concatenation)).collect(Collectors.toList());
            sb.append(StringUtil.join(addSpace(concatenation_node(ctx).get().getText()), concatenationList));
        }

        return sb.toString();
    }

    private Optional<TerminalNode> concatenation_node(ConcatenationContext concatenation) {
        return Arrays.asList(
                concatenation.ASTERISK(),
                concatenation.SOLIDUS(),
                concatenation.PLUS_SIGN(),
                concatenation.MINUS_SIGN())
                .stream().filter(node -> node != null).findFirst();
    }

    private String unary_expression(Unary_expressionContext ctx) {
        StringBuilder sb = new StringBuilder();

        Unary_expressionContext unary_expression = ctx.unary_expression();
        if (unary_expression != null) {
            sb.append(unary_expression(unary_expression));
        }

        sb.append(ctx.case_statement() != null ? ctx.case_statement().getText() : StringUtil.EMPTY_STRING);
        sb.append(ctx.quantified_expression() != null ? ctx.quantified_expression().getText() : StringUtil.EMPTY_STRING);
        sb.append(ctx.standard_function() != null ? ctx.standard_function().getText() : StringUtil.EMPTY_STRING);

        AtomContext atom = ctx.atom();
        if (atom != null) {
            sb.append(atom.table_element() != null ? atom.table_element().getText() : StringUtil.EMPTY_STRING);
            sb.append(atom.outer_join_sign() != null ? atom.outer_join_sign().getText() : StringUtil.EMPTY_STRING);
            sb.append(atom.bind_variable() != null ? atom.bind_variable().getText() : StringUtil.EMPTY_STRING);
            sb.append(atom.constant() != null ? atom.constant().getText() : StringUtil.EMPTY_STRING);
            sb.append(atom.general_element() != null ? atom.general_element().getText() : StringUtil.EMPTY_STRING);
            sb.append(atom.LEFT_PAREN() != null ? atom.LEFT_PAREN().getText() : StringUtil.EMPTY_STRING);
            sb.append(atom.subquery() != null ? atom.subquery().getText() : StringUtil.EMPTY_STRING);

            ExpressionsContext expressions = atom.expressions();
            if (expressions != null && expressions.expression() != null) {
                expressions.expression().forEach(expression -> sb.append(Logical_expression_parse(expression.logical_expression())));
            }

            sb.append(atom.RIGHT_PAREN() != null ? atom.RIGHT_PAREN().getText() : StringUtil.EMPTY_STRING);
        }

        return sb.toString();
    }

    private String between_elements(Between_elementsContext between_element) {
        StringBuilder sb = new StringBuilder();

        List<String> in_elements = between_element.concatenation()
                .stream()
                .map(concatenation -> concatenation.getText()).collect(Collectors.toList());
        sb.append(StringUtil.join(addSpace(between_element.AND().getText()), in_elements));

        return sb.toString();
    }

    private String in_elements(In_elementsContext in_element) {
        StringBuilder sb = new StringBuilder();

        sb.append(in_element.LEFT_PAREN().getText());
        List<String> in_elements = in_element.concatenation()
                .stream()
                .map(concatenation -> concatenation.getText()).collect(Collectors.toList());
        sb.append(StringUtil.join(String.valueOf(StringUtil.COMMA), in_elements));

        sb.append(in_element.RIGHT_PAREN().getText());
        return sb.toString();
    }

    private Optional<TerminalNode> multiset_expression_node(Compound_expressionContext compound_expression) {
        return Arrays.asList(
                compound_expression.IN(),
                compound_expression.BETWEEN())
                .stream().filter(node -> node != null).findFirst();
    }

    private Optional<TerminalNode> multiset_expression_node_middle(Compound_expressionContext compound_expression) {
        return Arrays.asList(
                compound_expression.NOT(),
                compound_expression.LIKE(),
                compound_expression.LIKEC(),
                compound_expression.LIKE2(),
                compound_expression.LIKE4(),
                compound_expression.ESCAPE())
                .stream().filter(node -> node != null).findFirst();
    }

    private String logical_operation_expression(Unary_logical_expressionContext unary_logical_expression) {

        StringBuilder sb = new StringBuilder();

        List<Logical_operationContext> logical_operations = unary_logical_expression.logical_operation();

        logical_operations.forEach(logical_operation -> {
            logical_operation_left_node(logical_operation).ifPresent(node -> sb.append(addSpace(node.getText())));
            sb.append(addSpace(type_spec_operation(logical_operation)));
            logical_operation_right_node(logical_operation).ifPresent(node -> sb.append(addSpace(node.getText())));
        });

        return sb.toString();
    }

    private List<TerminalNode> unary_logical_expression_node(Unary_logical_expressionContext unary_logical_expression) {
        return Arrays.asList(unary_logical_expression.IS(), unary_logical_expression.NOT())
                .stream()
                .filter(nodeList -> nodeList != null && nodeList.size() > 0)
                .flatMap(node -> node.stream())
                .collect(Collectors.toList());
    }

    private String type_spec_operation(Logical_operationContext logical_operation) {
        StringBuilder sb = new StringBuilder();

        List<Type_specContext> type_specs = logical_operation.type_spec();
        type_specs.forEach(type_spec -> {
            type_spec_node(type_spec).ifPresent(node -> sb.append(addSpace(node.getText())).append(type_spec.type_name().getText()));
        });

        return sb.toString();
    }

    private Optional<TerminalNode> logical_operation_left_node(Logical_operationContext logical_operation) {
        return Arrays.asList(
                logical_operation.NULL_(),
                logical_operation.NAN(),
                logical_operation.PRESENT(),
                logical_operation.INFINITE(),
                logical_operation.A_LETTER(),
                logical_operation.SET(),
                logical_operation.EMPTY(),
                logical_operation.OF(),
                logical_operation.LEFT_PAREN())
                .stream().filter(node -> node != null).findFirst();
    }

    private Optional<TerminalNode> logical_operation_right_node(Logical_operationContext logical_operation) {
        return Arrays.asList(
                logical_operation.RIGHT_PAREN(),
                logical_operation.TYPE(),
                logical_operation.ONLY(),
                logical_operation.COMMA() != null && logical_operation.COMMA().size() > 0 ? logical_operation.COMMA(0) : null)
                .stream().filter(node -> node != null).findFirst();
    }

    private Optional<TerminalNode> type_spec_node(Type_specContext type_spec) {
        return Arrays.asList(
                type_spec.REF(),
                type_spec.PERCENT_ROWTYPE(),
                type_spec.PERCENT_TYPE())
                .stream().filter(node -> node != null).findFirst();
    }
}
