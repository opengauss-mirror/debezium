/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package io.debezium.migration;

/**
 * Description: ObjectEnum
 *
 * @author jianghongbo
 * @since 2025/02/05
 */
public enum ObjectEnum {
    VIEW("VIEW"),
    FUNCTION("FUNCTION"),
    TRIGGER("TRIGGER"),
    PROCEDURE("PROCEDURE");

    private String objType;

    ObjectEnum(String objType) {
        this.objType = objType;
    }
    public String code() {
        return objType;
    }
}
