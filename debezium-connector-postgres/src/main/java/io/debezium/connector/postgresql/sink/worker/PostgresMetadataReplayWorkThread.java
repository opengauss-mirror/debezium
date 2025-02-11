/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package io.debezium.connector.postgresql.sink.worker;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.StringJoiner;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;

import io.debezium.connector.postgresql.migration.MigrationUtil;
import io.debezium.connector.postgresql.migration.PostgresSqlConstant;
import io.debezium.connector.postgresql.sink.common.SourceDataField;
import io.debezium.connector.postgresql.sink.object.ConnectionInfo;
import io.debezium.connector.postgresql.sink.object.SourceField;
import io.debezium.connector.postgresql.sink.record.SinkMetadataRecord;
import io.debezium.connector.postgresql.sink.utils.ConnectionUtil;
import io.debezium.metadata.column.PostgresColumnType;
import io.debezium.sink.worker.ReplayWorkThread;

/**
 * PostgresMetadataReplayWorkThread class
 *
 * @author jianghongbo
 * @since 2025/2/6
 */
public class PostgresMetadataReplayWorkThread extends ReplayWorkThread {
    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresMetadataReplayWorkThread.class);
    private static final String LINESEP = System.lineSeparator();
    private static final String CREATETABLE_TEMP = "DROP TABLE IF EXISTS %s.%s; CREATE TABLE %s.%s ( %s ) %s";
    private static final Map<String, String> typeConvertMap = new HashMap<String, String>() {
        {
            put("line", "varchar");
            put("pg_lsn", "varchar");
        }
    };

    private final BlockingQueue<SinkMetadataRecord> sinkMetaRecordQueue = new LinkedBlockingDeque<>();
    private final Map<String, String> schemaMappingMap;
    private final ConnectionInfo connectionInfo;
    private final Map<String, String> withOptionsMap;

    private Connection connection;
    private Statement statement = null;
    private Map<String, Boolean> schemaCreateMap = new HashMap<>();
    private boolean isRunning = false;
    private boolean shouldStop = false;
    private List<String> createdTables = new ArrayList<>();

    /**
     * Constructor
     *
     * @param schemaMappingMap Map<String, String> the schema mapping map
     * @param connectionInfo ConnectionInfo the connection information
     * @param createTableWithOptions String WithOptions information
     */
    public PostgresMetadataReplayWorkThread(Map<String, String> schemaMappingMap, ConnectionInfo connectionInfo,
                                            String createTableWithOptions) {
        super(1);
        this.schemaMappingMap = schemaMappingMap;
        this.connectionInfo = connectionInfo;
        this.withOptionsMap = initWithOptionsMap(createTableWithOptions);
    }

    private Map<String, String> initWithOptionsMap(String createTableWithOptions) {
        Map<String, String> optionsMap = new HashMap<>();
        if (createTableWithOptions != null) {
            try {
                JSONArray withOptions = JSONArray.parseArray(createTableWithOptions);
                for (int i = 0; i < withOptions.size(); i++) {
                    JSONObject withOption = withOptions.getJSONObject(i);
                    String table = withOption.getString("table");
                    String with = withOption.getString("with");
                    optionsMap.put(table, with);
                }
            } catch (JSONException e) {
                LOGGER.error("An error occurred while initializing with options", e);
            }
        }
        return optionsMap;
    }

    @Override
    public void run() {
        SinkMetadataRecord sinkMetadataRecord;
        connection = ConnectionUtil.createConnection(connectionInfo);
        statement = createStatement(connection, 1);
        while (true) {
            try {
                if (shouldStop && sinkMetaRecordQueue.isEmpty()) {
                    closeResource();
                    LOGGER.info("metaData work thread exiting");
                    break;
                }
                sinkMetadataRecord = sinkMetaRecordQueue.take();
                // child table must wait parent table created
                List<String> parentTables = parseParents(sinkMetadataRecord);
                if (canCreateTable(parentTables)) {
                    createDestTable(sinkMetadataRecord);
                } else {
                    sinkMetaRecordQueue.put(sinkMetadataRecord);
                }
            } catch (InterruptedException e) {
                LOGGER.error("take sinkMetaRecordQueue occurred InterruptedException", e);
            }
        }
    }

    private void closeResource() {
        try {
            if (statement != null) {
                statement.close();
            }
            if (connection != null) {
                connection.close();
            }
        } catch (SQLException e) {
            LOGGER.error("metaDta thread close connection occurred SQLException", e);
        }
    }

    private List<String> parseParents(SinkMetadataRecord sinkMetadataRecord) {
        String parentsStr = sinkMetadataRecord.getParents();
        if ("".equals(parentsStr)) {
            return new ArrayList<>();
        }
        List<String> parents = new ArrayList<>();
        String schema = sinkMetadataRecord.getSchemaName();
        Arrays.asList(parentsStr.split(",")).forEach((parentTable) -> {
            parents.add(schema + "." + parentTable);
        });
        return parents;
    }

    private boolean canCreateTable(List<String> parentTables) {
        boolean canCreate = true;
        for (String parent : parentTables) {
            if (!createdTables.contains(parent)) {
                canCreate = false;
                break;
            }
        }
        return canCreate;
    }

    private void createDestTable(SinkMetadataRecord sinkMetadataRecord) throws InterruptedException {
        String schema = schemaMappingMap.getOrDefault(sinkMetadataRecord.getSchemaName(),
                sinkMetadataRecord.getSchemaName());
        String table = sinkMetadataRecord.getTableName();
        if (schemaCreateMap.get(schema) == null) {
            createDestSchema(schema);
        }

        String createStmt = constructCreateStmt(sinkMetadataRecord);
        try {
            MigrationUtil.switchSchema(schemaMappingMap.getOrDefault(schema, schema), connection);
            statement.execute(createStmt);
            createdTables.add(schema + "." + table);
            LOGGER.info("create table success, create statement: {}", createStmt);
        } catch (SQLException e) {
            LOGGER.error("create table occured SQLException, error sql: {}", createStmt, e);
        }
    }

    private void createDestSchema(String schema) {
        try {
            statement.execute(String.format(PostgresSqlConstant.SCHEMACREATE, schema));
            schemaCreateMap.put(schema, true);
        } catch (SQLException e) {
            LOGGER.error("create dest schema occured SQLException, error sql: "
                    + String.format(PostgresSqlConstant.SCHEMACREATE, schema), e);
        }
    }

    /**
     * Adds data
     *
     * @param sinkMetadataRecord SinkRecordObject the sinkRecordObject
     */
    public void addData(SinkMetadataRecord sinkMetadataRecord) {
        sinkMetaRecordQueue.add(sinkMetadataRecord);
    }

    /**
     * Adds metadata
     *
     * @param sinkMetadataRecord SinkMetadataRecord
     */
    public void addMetaData(SinkMetadataRecord sinkMetadataRecord) {
        sinkMetaRecordQueue.add(sinkMetadataRecord);
    }

    /**
     * Gets connection.
     *
     * @return the value of connection
     */
    public Connection getConnection() {
        return connection;
    }

    /**
     * Sets the connection.
     *
     * @param connection the connection
     */
    public void setConnection(Connection connection) {
        this.connection = connection;
    }

    /**
     * construct create table sql.
     *
     * @param sinkMetadataRecord SinkMetadataRecord
     * @return String
     */
    public String constructCreateStmt(SinkMetadataRecord sinkMetadataRecord) {
        List<Struct> tableChanges = sinkMetadataRecord.getTableChanges();
        if (tableChanges.size() != 1) {
            throw new IllegalArgumentException("Field tableChanges length should be 1");
        }
        checkStruct(tableChanges.get(0).get(SourceField.TABLE));
        Struct tableMeta = (Struct) tableChanges.get(0).get(SourceField.TABLE);
        StringJoiner columnDdl = new StringJoiner(", ");
        for (Object columnMeta : tableMeta.getArray(SourceDataField.COLUMNMETA)) {
            checkStruct(columnMeta);
            Struct columnStruct = (Struct) columnMeta;
            String colName = columnStruct.getString("name");
            String colType = getColumnType(columnStruct);
            String nullType = columnStruct.getBoolean("optional") ? "" : " NOT NULL ";
            columnDdl.add(String.format("%s %s %s", colName, colType, nullType));
        }
        String defColumns = columnDdl.toString();
        String partitionDdl = sinkMetadataRecord.getPartition();
        String schema = sinkMetadataRecord.getSchemaName();
        String table = sinkMetadataRecord.getTableName();
        String targetSchema = schemaMappingMap.getOrDefault(schema, schema);
        String createStmt = String.format(CREATETABLE_TEMP, targetSchema, table, targetSchema, table,
                defColumns, partitionDdl);
        // table have parents should inherit parents
        StringBuilder createStmtBuilder = new StringBuilder(createStmt);
        appendInheritsDdl(createStmtBuilder, sinkMetadataRecord);
        appendWithOptions(createStmtBuilder, table);
        return createStmtBuilder.toString();
    }

    private void checkStruct(Object object) {
        if (!(object instanceof Struct)) {
            throw new IllegalArgumentException("Expected a Struct but found " + object.getClass().getName());
        }
    }

    private void appendInheritsDdl(StringBuilder createStmtBuilder, SinkMetadataRecord sinkMetadataRecord) {
        String parents = sinkMetadataRecord.getParents();
        String inheritsDdl = "";
        if (!("".equals(parents))) {
            inheritsDdl = String.format(" Inherits (%s)", parents);
        }
        createStmtBuilder.append(inheritsDdl);
    }

    private void appendWithOptions(StringBuilder createStmtBuilder, String tableName) {
        if (withOptionsMap.containsKey(tableName)) {
            String withOptions = withOptionsMap.get(tableName);
            createStmtBuilder.append(" with (").append(withOptions).append(")");
        } else if (withOptionsMap.containsKey("*")) {
            String withOptions = withOptionsMap.get("*");
            createStmtBuilder.append(" with (").append(withOptions).append(")");
        }
    }

    private String getColumnType(Struct columnStruct) {
        String typeName = convertType(columnStruct.getString("typeName"));
        StringBuilder builder = new StringBuilder(typeName);
        // 判断是否有精度
        if (PostgresColumnType.isTypeWithLength(typeName)) {
            // 获取字段长度
            Integer length = columnStruct.getInt32("length");
            Integer scale = columnStruct.getInt32("scale");
            // time相关类型获取的是scale而不是length
            if (PostgresColumnType.isTimesTypes(typeName)) {
                builder.append("(").append(scale).append(")");
            } else if (PostgresColumnType.isTypesInterval(typeName)) {
                if (columnStruct.getString("intervalType") != null) {
                    builder.append(" " + columnStruct.getString("intervalType"));
                } else {
                    builder.append("(").append(scale).append(")");
                }
            } else {
                // 可变长类型length == Integer.MAX_VALUE时表示没指定长度, numberic类型length==0时没指定长度和精度。
                if ((PostgresColumnType.isVarsTypes(typeName) && length != Integer.MAX_VALUE)
                        || (PostgresColumnType.isNumericType(typeName) && length > 0)
                        || (!PostgresColumnType.isVarsTypes(typeName) && !PostgresColumnType.isNumericType(typeName))) {
                    builder.append("(").append(length);
                }
                // numeric类型获取scale
                if (PostgresColumnType.isNumericType(typeName) && length > 0 && scale != null && scale > 0) {
                    builder.append(",").append(scale);
                }
                if ((PostgresColumnType.isVarsTypes(typeName) && length != Integer.MAX_VALUE)
                        || (PostgresColumnType.isNumericType(typeName) && length != 0)
                        || (!PostgresColumnType.isVarsTypes(typeName) && !PostgresColumnType.isNumericType(typeName))) {
                    builder.append(")");
                }
            }
        }
        // 序列类型不显示指定默认值
        if (!PostgresColumnType.isSerialTypes(typeName)
                && columnStruct.getString("defaultValueExpression") != null) {
            builder.append(" default " + columnStruct.getString("defaultValueExpression"));
        }
        return builder.toString();
    }

    private String convertType(String typeName) {
        if (typeConvertMap.get(typeName) != null) {
            return typeConvertMap.get(typeName);
        }
        return typeName;
    }

    public boolean isRunning() {
        return isRunning;
    }

    public void setRunning(boolean isRunning) {
        this.isRunning = isRunning;
    }

    public boolean isShouldStop() {
        return shouldStop;
    }

    public void setShouldStop(boolean shouldStop) {
        this.shouldStop = shouldStop;
    }
}
