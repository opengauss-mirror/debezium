/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.sink.event;

import java.io.IOException;

import com.github.shyiko.mysql.binlog.event.deserialization.GtidEventDataDeserializer;
import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream;

/**
 * Description: MyGtidEventDataDeserializer class
 * @author douxin
 * @date 2022/10/21
 **/
public class MyGtidEventDataDeserializer extends GtidEventDataDeserializer {
    private static final int LOGICAL_PARALLEL_REPLICATION_FLAG = 2;

    @Override
    public MyGtidEventData deserialize(ByteArrayInputStream inputStream) throws IOException {
        MyGtidEventData eventData = new MyGtidEventData();
        byte flags = (byte) inputStream.readInteger(1);
        byte[] sid = inputStream.read(16);
        long gno = inputStream.readLong(8);
        eventData.setFlags(flags);
        eventData.setGtid(this.byteArrayToHex(sid, 0, 4) + "-"
                + this.byteArrayToHex(sid, 4, 2) + "-"
                + this.byteArrayToHex(sid, 6, 2) + "-"
                + this.byteArrayToHex(sid, 8, 2) + "-"
                + this.byteArrayToHex(sid, 10, 6) + ":" + String.format("%d", gno));
        byte ltType = (byte) inputStream.readInteger(1);
        if (ltType == LOGICAL_PARALLEL_REPLICATION_FLAG) {
            long lastCommitted = inputStream.readLong(8);
            long sequenceNumber = inputStream.readLong(8);
            eventData.setLastCommitted(lastCommitted);
            eventData.setSequenceNumber(sequenceNumber);
        }
        return eventData;
    }

    private String byteArrayToHex(byte[] array, int offset, int len) {
        StringBuilder sb = new StringBuilder();
        for (int idx = offset; idx < offset + len && idx < array.length; ++idx) {
            sb.append(String.format("%02x", array[idx] & 255));
        }
        return sb.toString();
    }
}
