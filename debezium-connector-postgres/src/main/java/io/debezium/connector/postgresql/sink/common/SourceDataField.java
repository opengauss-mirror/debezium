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

package io.debezium.connector.postgresql.sink.common;

import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.data.Envelope;

/**
 * SourceDataField
 *
 * @author tianbin
 * @since 2024/11/26
 */
public class SourceDataField implements Cloneable {
    private static final Logger LOGGER = LoggerFactory.getLogger(SourceDataField.class);

    /**
     * Database
     */
    public static final String DATABASE = "db";

    /**
     * Schema
     */
    public static final String SCHEMA = "schema";

    /**
     * Table
     */
    public static final String TABLE = "table";

    /**
     * lsn
     */
    public static final String LSN = "lsn";

    /**
     * columns
     */
    public static final String COLUMNMETA = "columns";

    /**
     * msgType
     */
    public static final String MAGTYPE = "msgType";

    private String database;
    private String schema;
    private String table;
    private long lsn;

    /**
     * Constructor
     *
     * @param value Struct the value
     */
    public SourceDataField(Struct value) {
        if (value == null) {
            throw new IllegalArgumentException("value can't be null!");
        }
        Struct source = value.getStruct(Envelope.FieldName.SOURCE);
        if (source == null) {
            throw new IllegalArgumentException("source can't be null!");
        }

        this.database = source.getString(DATABASE);
        this.schema = source.getString(SCHEMA);
        this.table = source.getString(TABLE);
        this.lsn = source.getInt64(LSN);
    }

    /**
     * Gets database
     *
     * @return String the database
     */
    public String getDatabase() {
        return database;
    }

    /**
     * Sets database
     *
     * @param database String the database
     */
    public void setDatabase(String database) {
        this.database = database;
    }

    /**
     * Gets schema
     *
     * @return String the schema
     */
    public String getSchema() {
        return schema;
    }

    /**
     * Sets schema
     *
     * @param schema String the schema
     */
    public void setSchema(String schema) {
        this.schema = schema;
    }

    /**
     * Gets table
     *
     * @return String the table
     */
    public String getTable() {
        return table;
    }

    /**
     * Sets table
     *
     * @param table String the table
     */
    public void setTable(String table) {
        this.table = table;
    }

    /**
     * Sets lsn
     *
     * @param lsn long the lsn
     */
    public void setLsn(long lsn) {
        this.lsn = lsn;
    }

    /**
     * Gets lsn
     *
     * @return long th lsn
     */
    public long getLsn() {
        return lsn;
    }

    @Override
    public SourceDataField clone() {
        try {
            return (SourceDataField) super.clone();
        } catch (CloneNotSupportedException exp) {
            LOGGER.error("Clone source field failed.", exp);
        }
        return null;
    }

    @Override
    public String toString() {
        return "SourceField{"
                + "database='" + database + '\''
                + ", schema='" + schema + '\''
                + ", table='" + table + '\''
                + ", lsn='" + lsn + '\''
                + '}';
    }
}
