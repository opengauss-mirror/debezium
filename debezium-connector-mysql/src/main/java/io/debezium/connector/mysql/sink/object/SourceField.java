/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.sink.object;

import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Description: SourceField class
 * @author douxin
 * @date 2022/10/28
 **/
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
     * Table
     */
    public static final String TABLE = "table";

    /**
     * Gtid
     */
    public static final String GTID = "gtid";

    /**
     * Last_committed
     */
    public static final String LAST_COMMITTED = "last_committed";

    /**
     * Sequence_number
     */
    public static final String SEQUENCE_NUMBER = "sequence_number";

    /**
     * Binlog file
     */
    public static final String FILE = "file";

    /**
     * Binlog position
     */
    public static final String POSITION = "pos";

    private String database;
    private String table;
    private String gtid;
    private long lastCommitted;
    private long sequenceNumber;
    private String file;
    private long position;

    /**
     * Constructor
     *
     * @param Struct the value
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
        this.table = source.getString(SourceField.TABLE);
        this.gtid = source.getString(SourceField.GTID);
        this.lastCommitted = source.getInt64(SourceField.LAST_COMMITTED);
        this.sequenceNumber = source.getInt64(SourceField.SEQUENCE_NUMBER);
        this.file = source.getString(SourceField.FILE);
        this.position = source.getInt64(SourceField.POSITION);
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
     * @param String the database
     */
    public void setDatabase(String database) {
        this.database = database;
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
     * @param String the table
     */
    public void setTable(String table) {
        this.table = table;
    }

    /**
     * Gets gtid
     *
     * @return String the gtid
     */
    public String getGtid() {
        return gtid;
    }

    /**
     * Sets gtid
     *
     * @param String the gitd
     */
    public void setGtid(String gitd) {
        this.gtid = gitd;
    }

    /**
     * Gets last committed
     *
     * @return long the last committed
     */
    public long getLastCommittd() {
        return lastCommitted;
    }

    /**
     * Sets last committed
     *
     * @param long the last committed
     */
    public void setLastCommittd(long lastCommitted) {
        this.lastCommitted = lastCommitted;
    }

    /**
     * Gets sequence number
     *
     * @return long the sequence number
     */
    public long getSequenceNumber() {
        return sequenceNumber;
    }

    /**
     * Sets sequence number
     *
     * @param long the sequence number
     */
    public void setSequenceNumber(long sequenceNumber) {
        this.sequenceNumber = sequenceNumber;
    }

    /**
     * Gets binlog file
     *
     * @return String the binlog file
     */
    public String getFile() {
        return file;
    }

    /**
     * Sets binlog file
     *
     * @param String the binlog file
     */
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
     * Sets binlog position
     *
     * @param long the binlog position
     */
    public void setPosition(long position) {
        this.position = position;
    }

    @Override
    public String toString() {
        return "SourceField{" +
                "database='" + database + '\'' +
                ", table='" + table + '\'' +
                ", gtid='" + gtid + '\'' +
                ", lastCommitted=" + lastCommitted +
                ", sequenceNumber=" + sequenceNumber +
                ", file='" + file + '\'' +
                ", position=" + position +
                '}';
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
