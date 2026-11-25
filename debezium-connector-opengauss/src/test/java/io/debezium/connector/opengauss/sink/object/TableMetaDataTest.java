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

package io.debezium.connector.opengauss.sink.object;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class TableMetaDataTest {
    @Test
    public void test(){
        String schemaName = "test_wzy";
        String tableName = "test1";
        List<ColumnMetaData> columnList = new ArrayList<>();
        columnList.add(new ColumnMetaData("id", "int"));
        columnList.add(new ColumnMetaData("name", "char(20)"));
        columnList.add(new ColumnMetaData("role", "char(20)"));
        TableMetaData tableMetaData = new TableMetaData(schemaName, tableName, columnList);
        Assert.assertNotNull(tableMetaData);
    }
}
