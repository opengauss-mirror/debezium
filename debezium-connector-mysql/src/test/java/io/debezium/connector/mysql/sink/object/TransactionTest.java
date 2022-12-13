/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.sink.object;

import java.util.ArrayList;

import org.junit.Assert;
import org.junit.Test;

/**
 * Description: TransactionTest class
 *
 * @author douxin
 * @date 2022/11/25
 **/
public class TransactionTest {
    @Test
    public void test() {
        Transaction transaction = new Transaction();
        SourceField sourceField = null;
        transaction.setSourceField(sourceField);
        ArrayList<String> sqlList = new ArrayList<>();
        sqlList.add("create table test(id int)");
        transaction.setSqlList(sqlList);
        Assert.assertTrue(transaction.getIsDml());
        Assert.assertNull(transaction.getSourceField());
        Assert.assertEquals(1, transaction.getSqlList().size());
        transaction.setIsDml(false);
        Assert.assertFalse(transaction.getIsDml());
        Assert.assertTrue(transaction.toString().contains("isDml"));
    }
}
