/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.sink;

import org.junit.Assert;
import org.junit.Test;

import io.debezium.connector.oracle.sink.object.DdlOperation;

/**
 * Description: DdlOperationTest class
 *
 * @author gbase
 * @date 2023/08/02
 **/
public class DdlOperationTest {
    @Test
    public void test() {
        String ddl1 = "create table test(id int)";
        DdlOperation ddlOperation = new DdlOperation(ddl1);
        Assert.assertEquals(ddl1 + ";", ddlOperation.getDdl());

        String ddl2 = "create table test1(id int)";
        ddlOperation.setDdl(ddl2);
        Assert.assertEquals(ddl2 + ";", ddlOperation.getDdl());

        Assert.assertFalse(ddlOperation.getIsDml());
        Assert.assertEquals("ddl", DdlOperation.DDL);
        Assert.assertTrue(ddlOperation.toString().contains("ddl"));
    }
}
