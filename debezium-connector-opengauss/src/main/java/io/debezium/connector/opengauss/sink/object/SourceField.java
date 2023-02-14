/**
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.opengauss.sink.object;

import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Description: source field class
 * @author wangzhengyuan
 * @date 2022/11/04
 */
public class SourceField implements Cloneable {
    private static final Logger LOGGER = LoggerFactory.getLogger(SourceField.class);

    /**
     * Source
     */
    public static final String SOURCE = "source";

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

    

    private String database;
    private String schema;
    private String table;
    private String file;
    private long position;


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

        this.database = source.getString(SourceField.DATABASE);
        this.schema = source.getString(SourceField.SCHEMA);
        this.table = source.getString(SourceField.TABLE);

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
     * Gets binlog file
     *
     * @return String the binlog file
     */
    public String getFile() {
        return file;
    }


    public void setFile(String file) {
        this.file = file;
    }

    /**
     * Gets binlog position
     *
     * @return long the binlog position
     */
    public long getPosition() {
        return position;
    }

    /**
     * Sets position
     *
     * @param position long the position
     */
    public void setPosition(long position) {
        this.position = position;
    }

    @Override
    public SourceField clone() {
        try {
            return (SourceField) super.clone();
        }
        catch (CloneNotSupportedException exp) {
            LOGGER.error("Clone source field failed.", exp);
        }
        return null;
    }
}
