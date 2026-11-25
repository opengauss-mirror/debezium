/*
 * Copyright 2026 Huawei Technologies Co., Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
