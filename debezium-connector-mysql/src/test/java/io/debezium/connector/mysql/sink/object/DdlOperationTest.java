/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.sink.object;

import org.junit.Assert;
import org.junit.Test;

/**
 * Description: DdlOperationTest class
 *
 * @author douxin
 * @date 2022/11/25
 **/
public class DdlOperationTest {
    @Test
    public void test() {
        DdlOperation ddlOperation = new DdlOperation("create table test(id int);");
        ddlOperation.setDdl("create table test1(id int);");
        Assert.assertEquals("create table test1(id int);", ddlOperation.getDdl());
        Assert.assertFalse(ddlOperation.getIsDml());
        Assert.assertEquals("ddl", DdlOperation.DDL);
        Assert.assertTrue(ddlOperation.toString().contains("ddl"));
    }
}
