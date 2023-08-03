/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.sink.object;

import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Description: Transaction class
 * @author douxin
 * @date 2022/10/27
 **/
public class Transaction implements Cloneable {
    private static final Logger LOGGER = LoggerFactory.getLogger(Transaction.class);

    private SourceField sourceField;
    private ArrayList<String> sqlList = new ArrayList<>();
    private boolean isDml = true;
    private Long txnBeginOffset;
    private Long txnEndOffset;
    private String expMessage;

    /**
     * Constructor
     */
    public Transaction() {
    }

    /**
     * Sets source field
     *
     * @param SourceField the source field
     */
    public void setSourceField(SourceField sourceField) {
        this.sourceField = sourceField;
    }

    /**
     * Gets source field
     *
     * @return SourceField the source field
     */
    public SourceField getSourceField() {
        return sourceField;
    }

    /**
     * Sets sql list
     *
     * @param ArrayList<String> the sql list
     */
    public void setSqlList(ArrayList<String> sqlList) {
        this.sqlList = sqlList;
    }

    /**
     * Gets sql list
     *
     * @return ArrayList<String> the sql list
     */
    public ArrayList<String> getSqlList() {
        return sqlList;
    }

    /**
     * Gets txnBeginOffset.
     *
     * @return the value of txnBeginOffset
     */
    public Long getTxnBeginOffset() {
        return txnBeginOffset;
    }

    /**
     * Sets the txnBeginOffset.
     *
     * @param txnBeginOffset txnBeginOffset
     */
    public void setTxnBeginOffset(Long txnBeginOffset) {
        this.txnBeginOffset = txnBeginOffset;
    }

    /**
     * Gets txnEndOffset.
     *
     * @return the value of txnEndOffset
     */
    public Long getTxnEndOffset() {
        return txnEndOffset;
    }

    /**
     * Sets the txnEndOffset.
     *
     * @param txnEndOffset txnEndOffset
     */
    public void setTxnEndOffset(Long txnEndOffset) {
        this.txnEndOffset = txnEndOffset;
    }

    /**
     * Sets is dml
     *
     * @param boolean true if is dml
     */
    public void setIsDml(boolean dml) {
        isDml = dml;
    }

    /**
     * Gets is dml
     *
     * @return boolean true if is dml
     */
    public boolean getIsDml() {
        return isDml;
    }

    /**
     * interleave with other transaction
     *
     * @param Transaction the other transaction
     * @return boolean true if can interleave with other transaction
     */
    public boolean interleaved(Transaction other) {
        return other.getSourceField().getSequenceNumber() > this.getSourceField().getLastCommittd();
    }

    /**
     * Gets exception message
     *
     * @return String the exception message
     */
    public String getExpMessage() {
        return expMessage.replaceAll(System.lineSeparator(), " ");
    }

    /**
     * Sets exception message
     *
     * @param expMessage String the exception message
     */
    public void setExpMessage(String expMessage) {
        this.expMessage = expMessage;
    }

    @Override
    public String toString() {
        return "Transaction{"
                + "sourceField=" + sourceField
                + ", sqlList=" + sqlList
                + ", isDml=" + isDml
                + ", txnBeginOffset=" + txnBeginOffset
                + ", txnEndOffset=" + txnEndOffset
                + '}';
    }

    @Override
    public Transaction clone() {
        Transaction transaction = null;
        try {
            transaction = (Transaction) super.clone();
            transaction.setSourceField(this.sourceField.clone());
            transaction.setSqlList(new ArrayList<>(this.sqlList));
        }
        catch (CloneNotSupportedException exp) {
            LOGGER.error("Clone transaction failed.", exp);
        }
        return transaction;
    }
}
