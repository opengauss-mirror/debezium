/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.antlr.listener;

import io.debezium.connector.mysql.antlr.MySqlAntlrDdlParser;
import io.debezium.ddl.parser.mysql.generated.MySqlParser;
import io.debezium.ddl.parser.mysql.generated.MySqlParserBaseListener;
import io.debezium.relational.TableEditor;
import io.debezium.relational.TableId;
import io.debezium.text.ParsingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.debezium.antlr.AntlrDdlParser.getText;

/**
 * Description: CreateIndexParserListener class
 *
 * @author douxin
 * @since 2023/12/19
 **/
public class CreateIndexParserListener extends MySqlParserBaseListener {
    private static final Logger LOG = LoggerFactory.getLogger(AlterTableParserListener.class);

    private final MySqlAntlrDdlParser parser;

    public CreateIndexParserListener(MySqlAntlrDdlParser parser) {
        this.parser = parser;
    }

    @Override
    public void enterCreateIndex(MySqlParser.CreateIndexContext ctx) {
        // for not unique index
        if (ctx.UNIQUE() == null) {
            TableId tableId = parser.parseQualifiedTableId(ctx.tableName().fullId());
            if (!parser.getTableFilter().isIncluded(tableId)) {
                LOG.debug("{} is not monitored, no need to process index", tableId);
                return;
            }
            TableEditor tableEditor = parser.databaseTables().editTable(tableId);
            if (tableEditor != null) {
                parser.signalCreateIndex(parser.parseName(ctx.uid()), tableId, ctx);
            }
            else {
                throw new ParsingException(null, "Trying to create index on non existing table " + tableId.toString()
                        + ". Query: " + getText(ctx));
            }
        }
        super.enterCreateIndex(ctx);
    }
}

