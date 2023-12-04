/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.sink.object;

import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Description: SourceField class
 *
 * @author gbase
 * @date 2023/07/28
 **/
public class SourceField implements Cloneable {
    private static final Logger LOGGER = LoggerFactory.getLogger(SourceField.class);

    /**
     * Source
     */
    public static final String SOURCE = "source";

    /**
     * The transaction id
     */
    public static final String TXID = "txId";

    /**
     * The SCN of the change.
     */
    public static final String SCN = "scn";

    /**
     * Describes the SCN of the transaction commit that the change event participates within.
     * This field is only present when using the LogMiner connection adapter.
     */
    public static final String COMMIT_SCN = "commit_scn";

    /**
     * Whether the event is part of an ongoing snapshot or not.
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
     * Sequence
     */
    public static final String SEQUENCE = "sequence";

    private String snapshot;
    private String database;
    private String schema;
    private String table;
    private String txId;
    private String scn;
    private String commitScn;

    /**
     * Constructor
     *
     * @param value source value
     */
    public SourceField(Struct value) {
        if (value == null) {
            throw new IllegalArgumentException("value can't be null!");
        }
        Struct source = value.getStruct(SourceField.SOURCE);
        if (source == null) {
            throw new IllegalArgumentException("source can't be null!");
        }
        this.snapshot = source.getString(SourceField.SNAPSHOT);
        this.database = source.getString(SourceField.DATABASE);
        this.schema = source.getString(SourceField.SCHEMA);
        this.table = source.getString(SourceField.TABLE);
        this.txId = source.getString(SourceField.TXID);
        this.scn = source.getString(SourceField.SCN);
        this.commitScn = source.getString(SourceField.COMMIT_SCN);
    }

    /**
     * Gets snapshot
     *
     * @return the snapshot
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

    /**
     * Gets database
     *
     * @return the database
     */
    public String getDatabase() {
        return database;
    }

    /**
     * Sets database
     *
     * @param database the database
     */
    public void setDatabase(String database) {
        this.database = database;
    }

    /**
     * Gets table
     *
     * @return the table
     */
    public String getTable() {
        return table;
    }

    /**
     * Sets table
     *
     * @param table the table
     */
    public void setTable(String table) {
        this.table = table;
    }

    /**
     * Gets transaction id
     *
     * @return the transaction id
     */
    public String getTxId() {
        return txId;
    }

    /**
     * Sets transaction id
     *
     * @param txId the transaction id
     */
    public void setTxId(String txId) {
        this.txId = txId;
    }

    /**
     * Gets schema
     *
     * @return the schema
     */
    public String getSchema() {
        return schema;
    }

    /**
     * Sets schema
     *
     * @param schema the schema
     */
    public void setSchema(String schema) {
        this.schema = schema;
    }

    /**
     * Gets scn
     *
     * @return the scn
     */
    public String getScn() {
        return scn;
    }

    /**
     * Sets scn
     *
     * @param scn the scn
     */
    public void setScn(String scn) {
        this.scn = scn;
    }

    /**
     * Gets commit scn
     *
     * @return the commit scn
     */
    public String getCommitScn() {
        return commitScn;
    }

    /**
     * Sets commit scn
     *
     * @param commitScn the commit scn
     */
    public void setCommitScn(String commitScn) {
        this.commitScn = commitScn;
    }

    @Override
    public String toString() {
        return "SourceField{" +
                "snapshot='" + snapshot + '\'' +
                ", database='" + database + '\'' +
                ", schema='" + schema + '\'' +
                ", table='" + table + '\'' +
                ", txId='" + txId + '\'' +
                ", scn='" + scn + '\'' +
                ", commitScn='" + commitScn + '\'' +
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
