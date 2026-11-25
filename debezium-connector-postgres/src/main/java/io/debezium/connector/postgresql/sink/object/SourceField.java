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

package io.debezium.connector.postgresql.sink.object;

import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * source field class
 *
 * @author tianbin
 * @since 2024-11-25
 */
public class SourceField implements Cloneable {
    private static final Logger LOGGER = LoggerFactory.getLogger(SourceField.class);

    /**
     * Source
     */
    public static final String SOURCE = "source";

    /**
     * Snapshot
     */
    public static final String SNAPSHOT = "snapshot";

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

    private String snapshot;
    private String database;
    private String schema;
    private String table;
    private long lsn;

    /**
     * Constructor
     *
     * @param value Struct the value
     */
    public SourceField(Struct value) {
        if (value == null) {
            throw new IllegalArgumentException("value can't be null!");
        }
        Struct source = value.getStruct(SourceField.SOURCE);
        if (source == null) {
            throw new IllegalArgumentException("source can't be null!");
        }

        this.snapshot = source.getString(SNAPSHOT);
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

    /**
     * Gets snapshot
     *
     * @return String the snapshot
     */
    public String getSnapshot() {
        return snapshot;
    }

    /**
     * Sets snapshot
     *
     * @param snapshot the snapshot
     */
    public void setSnapshot(String snapshot) {
        this.snapshot = snapshot;
    }

    @Override
    public SourceField clone() {
        try {
            return (SourceField) super.clone();
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

    /**
     * Determines if the data is full data.
     *
     * @return true if the data is full data, false otherwise.
     */
    public boolean isFullData() {
        return "true".equals(snapshot) || "last".equals(snapshot);
    }
}
