/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package io.debezium.connector.opengauss.sink.object;

import io.debezium.connector.opengauss.sink.ddl.DdlParser;
import io.debezium.connector.opengauss.sink.ddl.OpengaussDdlParser;
import io.debezium.connector.opengauss.sink.ddl.PostgresDdlParser;
import org.apache.kafka.connect.data.Struct;

import java.util.Map;

/**
 * Description: DdlOperation
 *
 * @author tianbin
 * @since 2024/11/05
 **/
public class DdlOperation extends DataOperation {
    /**
     * Ddl
     */
    public static final String DDL = "ddl";

    private static final String MESSAGE = "message";

    private static final String POSTGRES = "postgres";

    private static final String OPENGAUSS = "opengauss";

    private String rawDdl;
    private String ddl = "";
    private String identifier;
    private String operation;
    private DdlParser ddlParser;
    private boolean isTableSql;
    private String relyTable;
    /**
     * Constructor
     *
     * @param value the Struct value
     * @param schemaMappingMap the schema mapping map
     * @param databaseType the database type
     */
    public DdlOperation(Struct value, Map<String, String> schemaMappingMap, String databaseType) {
        if (value == null) {
            throw new IllegalArgumentException("value can't be null!");
        }
        this.operation = value.getString(OPERATION);
        Struct message = value.getStruct(MESSAGE);
        if (message == null) {
            throw new IllegalArgumentException("message can't be null!");
        }
        this.rawDdl = message.getString(DdlOperation.DDL);
        this.ddlParser = buildDdlParser(schemaMappingMap, databaseType);
        if (ddlParser != null) {
            this.ddl = ddlParser.parse(rawDdl);
            this.identifier = ddlParser.identifier();
            this.isTableSql = ddlParser.isTableRefreshed();
            this.relyTable = ddlParser.owner();
        }
        setIsDml(false);
    }

    @Override
    public String toString() {
        return "DdlOperation{" + "isDml=" + isDml + ", rawDdl='" + rawDdl + '\'' + '}';
    }

    /**
     * Get operation
     *
     * @return operation
     */
    @Override
    public String getOperation() {
        return operation;
    }

    private DdlParser buildDdlParser(Map<String, String> schemaMappingMap, String databaseType) {
        DdlParser parser = null;
        if (POSTGRES.equalsIgnoreCase(databaseType)) {
            parser = new PostgresDdlParser(schemaMappingMap);
        } else if (OPENGAUSS.equalsIgnoreCase(databaseType)) {
            parser = new OpengaussDdlParser(schemaMappingMap);
        }
        return parser;
    }

    /**
     * Get parsed ddl
     *
     * @return ddl
     */
    public String getDdl() {
        return ddl;
    }

    /**
     * Get changed identifier
     *
     * @return identifier
     */
    public String getFullName() {
        return identifier;
    }

    /**
     * Get whether it is a table-level DDL.
     *
     * @return true if it is a table-level DDL, false otherwise
     */
    public boolean isTableSql() {
        return isTableSql;
    }

    public String getRelyTable() {
        return relyTable;
    }
}
