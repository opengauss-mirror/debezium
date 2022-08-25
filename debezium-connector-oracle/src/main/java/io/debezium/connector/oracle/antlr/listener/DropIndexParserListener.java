/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.oracle.antlr.listener;

import io.debezium.connector.oracle.antlr.OracleDdlParser;
import io.debezium.ddl.parser.oracle.generated.PlSqlParser;
import io.debezium.relational.Index;
import io.debezium.relational.TableEditor;
import io.debezium.relational.TableId;

/**
 * @author saxisuer
 */
public class DropIndexParserListener extends BaseParserListener {

    private TableEditor tableEditor;
    private String catalogName;
    private String schemaName;
    private OracleDdlParser parser;
    private String indexName;
    private Index index;

    public DropIndexParserListener(String catalogName, String schemaName, OracleDdlParser parser) {
        this.catalogName = catalogName;
        this.schemaName = schemaName;
        this.parser = parser;
    }

    @Override
    public void enterDrop_index(PlSqlParser.Drop_indexContext ctx) {
        PlSqlParser.Index_nameContext indexNameContext = ctx.index_name();
        if (indexNameContext.id_expression() != null) {
            indexName = getTableOrColumnName(indexNameContext.id_expression().getText());
        }
        else {
            indexName = getTableOrColumnName(indexNameContext.identifier().id_expression().getText());
        }
        LOGGER.info("Enter drop index,index_name: {}", indexName);
        TableId tableId = parser.databaseTables().getTaleIdByIndex(indexName);
        if (tableId == null) {
            LOGGER.info("Ignoring DROP INDEX statement for unknown table");
            return;
        }
        if (parser.databaseTables().forTable(tableId) == null) {
            LOGGER.info("Ignoring DROP INDEX statement for non-captured table {}", tableId);
            return;
        }
        index = new Index();
        index.setIndexName(indexName);
        index.setTableId(tableId.identifier());
        index.setSchemaName(schemaName);
        index.setTableName(tableId.table());
        tableEditor = parser.databaseTables().editTable(tableId);
        tableEditor.clearColumnChange();
        tableEditor.clearConstraint();
        super.enterDrop_index(ctx);
    }

    @Override
    public void exitDrop_index(PlSqlParser.Drop_indexContext ctx) {
        parser.runIfNotNull(() -> {
            tableEditor.setChangeIndex(index).removeIndex(indexName);
            parser.databaseTables().overwriteTable(tableEditor.create());
            parser.databaseTables().unbindTableIndex(indexName, tableEditor.tableId());
            parser.signalDropIndex(indexName, tableEditor.tableId(), ctx);
        }, index, tableEditor);
        super.exitDrop_index(ctx);
    }
}
