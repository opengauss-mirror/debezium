/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package io.debezium.connector.postgresql.sink.record;

import io.debezium.migration.ObjectEnum;

/**
 * SinkObjectRecord
 *
 * @author tianbin
 * @since 2024/11/26
 */
public class SinkObjectRecord {
    private String ddl;
    private String objName;
    private String schema;

    private ObjectEnum objType;

    public void setDdl(String ddl) {
        this.ddl = ddl;
    }

    public String getDdl() {
        return ddl;
    }

    public String getObjName() {
        return objName;
    }

    public String getSchema() {
        return schema;
    }

    public void setObjName(String objName) {
        this.objName = objName;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public ObjectEnum getObjType() {
        return objType;
    }

    public void setObjType(ObjectEnum objType) {
        this.objType = objType;
    }
}
