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

package io.debezium.connector.opengauss.sink.object;

/**
 * Description: DataOperation
 *
 * @author tianbin
 * @since 2024/11/5
 **/
public abstract class DataOperation {
    /**
     * Operation
     */
    public static final String OPERATION = "op";

    /**
     * Is dml flag
     */
    protected boolean isDml;

    public abstract String getOperation();

    /**
     * Gets is dml
     *
     * @return boolean true if is dml
     */
    public boolean getIsDml() {
        return isDml;
    }

    /**
     * Sets is dml
     *
     * @param isDml true if is dml
     */
    public void setIsDml(boolean isDml) {
        this.isDml = isDml;
    }
}
