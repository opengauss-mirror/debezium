package io.debezium.connector.opengauss.sink.object;

import org.junit.Assert;
import org.junit.Test;

public class SourceFieldTest {
    @Test
    public void test(){
        Assert.assertEquals("source", SourceField.SOURCE);
        Assert.assertEquals("db", SourceField.DATABASE);
        Assert.assertEquals("table", SourceField.TABLE);
    }
}
