/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.opengauss;

import io.debezium.connector.common.AbstractPartitionTest;

public class OpengaussPartitionTest extends AbstractPartitionTest<OpengaussPartition> {

    @Override
    protected OpengaussPartition createPartition1() {
        return new OpengaussPartition("server1");
    }

    @Override
    protected OpengaussPartition createPartition2() {
        return new OpengaussPartition("server2");
    }
}
