/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.oracle.antlr.listener;

import java.util.List;
import java.util.Optional;

import org.antlr.v4.runtime.tree.ParseTreeListener;

import io.debezium.connector.oracle.antlr.OracleDdlParser;
import io.debezium.ddl.parser.oracle.generated.PlSqlParser;
import io.debezium.relational.TableEditor;

/**
 * @author saxisuer
 * parser for constraint state  line  create constraint using index part
 */
public class ConstraintStateParserListener extends BaseParserListener {

    private final TableEditor tableEditor;
    private final OracleDdlParser parser;
    private final List<ParseTreeListener> listeners;

    public ConstraintStateParserListener(OracleDdlParser parser, TableEditor tableEditor, List<ParseTreeListener> listeners) {
        this.parser = parser;
        this.tableEditor = tableEditor;
        this.listeners = listeners;
    }

    @Override
    public void enterConstraint_state(PlSqlParser.Constraint_stateContext ctx) {
        LOGGER.debug("enterConstraint_state");
        parser.runIfNotNull(() -> {
            Optional<ParseTreeListener> first = listeners.stream().filter(each -> each instanceof CreateIndexParserListener).findFirst();
            if (first.isPresent()) {
                CreateIndexParserListener createIndexParserListener = (CreateIndexParserListener) first.get();
                createIndexParserListener.setTableEditor(tableEditor);
            }
        }, tableEditor);
        super.enterConstraint_state(ctx);
    }
}
