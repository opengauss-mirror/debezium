/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package io.debezium.migration;

import java.sql.Connection;
import java.util.Map;

/**
 * ObjectDdlFactory
 *
 * @author jianghongbo
 * @since 2025/02/05
 */
public interface ObjectDdlFactory {
    /**
     * get create object ddl by object type.
     *
     * @param schema String
     * @param objType ObjectEnum
     * @param conn Connection
     * @return Map<String,String>
     */
    Map<String, String> generateObjectDdl(String schema, ObjectEnum objType, Connection conn);
}
