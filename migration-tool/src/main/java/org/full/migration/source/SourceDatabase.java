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

package org.full.migration.source;

import lombok.Data;
import lombok.NoArgsConstructor;

import org.full.migration.constants.CommonConstants;
import org.full.migration.coordinator.QueueManager;
import org.full.migration.jdbc.JdbcConnection;
import org.full.migration.object.Column;
import org.full.migration.object.DatabaseConfig;
import org.full.migration.object.DbObject;
import org.full.migration.object.SourceConfig;
import org.full.migration.object.Table;
import org.full.migration.object.TableData;
import org.full.migration.utils.HexConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * SourceDatabase
 *
 * @since 2025-03-15
 */
@Data
@NoArgsConstructor
public abstract class SourceDatabase {
    private static final Logger LOGGER = LoggerFactory.getLogger(SourceDatabase.class);
    private static final Integer EMPTY_TABLE_PAGE_ROWS = 100000;

    /**
     * connection
     */
    protected JdbcConnection connection;

    /**
     * sourceConfig
     */
    protected SourceConfig sourceConfig;

    /**
     * Constructor
     *
     * @param sourceConfig sourceConfig
     */
    public SourceDatabase(SourceConfig sourceConfig) {
        this.sourceConfig = sourceConfig;
    }

    /**
     * getQueryTableSql
     *
     * @param schema schema
     * @return queryTableSql
     */
    protected abstract String getQueryTableSql(String schema);

    /**
     * getLockStatement
     *
     * @param schema schema
     * @param table table
     * @return lockSql
     */
    protected abstract String getLockSql(String schema, String table);

    /**
     * getQueryObjectSql
     *
     * @param objectType objectType
     * @return queryObjectSql
     */
    protected abstract String getQueryObjectSql(String objectType);

    /**
     * convertDefinition
     *
     * @param definition definition
     * @return definition
     */
    protected abstract String convertDefinition(String definition);

    /**
     * getSchemaSet
     *
     * @return schemaSet
     */
    public Set<String> getSchemaSet() {
        return sourceConfig.getSchemaMappings()
            .values()
            .stream()
            .map(dbSchema -> dbSchema.split("\\."))
            .filter(arr -> arr.length == 2)
            .map(arr -> arr[1])
            .collect(Collectors.toSet());
    }

    /**
     * queryTableSet
     *
     * @param schemaSet schemaSet
     * @return tableSet
     */
    public Set<Table> queryTableSet(Set<String> schemaSet) {
        Set<Table> tables = new HashSet<>();
        try (Connection conn = connection.getConnection(sourceConfig.getDbConn());
            Statement stmt = conn.createStatement()) {
            for (String schema : schemaSet) {
                String queryTableSql = getQueryTableSql(schema);
                try (ResultSet rs = stmt.executeQuery(queryTableSql)) {
                    while (rs.next()) {
                        String tableName = rs.getString("TableName");
                        String schemaName = rs.getString("SchemaName");
                        long avgRowLength = rs.getLong("avgRowLength");
                        int rowCount = rs.getInt("tableRows");
                        tables.add(new Table(sourceConfig.getDbConn().getDatabase(), schemaName, tableName, rowCount,
                            avgRowLength));
                    }
                }
            }
        } catch (SQLException e) {
            LOGGER.error("fail to query table list, error message:{}.", e.getMessage());
        }
        return tables;
    }

    /**
     * readTable
     *
     * @param table table
     * @param dbConfig dbConfig
     */
    public void readTable(Table table, DatabaseConfig dbConfig) {
        try (Connection conn = connection.getConnection(dbConfig)) {
            DatabaseMetaData metaData = conn.getMetaData();
            ResultSet columnMetadata = metaData.getColumns(table.getCatalogName(), table.getSchemaName(),
                table.getTableName(), null);
            List<Column> columns = new ArrayList<>();
            while (columnMetadata.next()) {
                readTableColumn(columnMetadata).ifPresent(columns::add);
            }
            if (table.getRowCount() == 0) {
                QueueManager.getInstance().addQueue(new TableData(table, columns, Collections.emptyList()));
                return;
            }
            QueueManager.getInstance().addQueue(new TableData(table, columns, extractData(table, columns, conn)));
        } catch (SQLException | InterruptedException e) {
            LOGGER.error("read table occurred an error, error message:{}", e.getMessage());
        }
    }

    private List<String> extractData(Table table, List<Column> columns, Connection conn) throws SQLException {
        Statement stmt = conn.createStatement();
        lockTable(table, stmt);
        List<String> columnNames = columns.stream().map(Column::getName).collect(Collectors.toList());
        int columnSize = columnNames.size();
        List<String> csvPathList = new ArrayList<>();
        String queryDataSql = String.format("SELECT %s FROM [%s].[%s].[%s] ", String.join(", ", columnNames),
            table.getCatalogName(), table.getSchemaName(), table.getTableName());
        ResultSet rs = stmt.executeQuery(queryDataSql);
        List<String> dataList = getDataList(rs, columnSize);
        String csvPath = getCsvFileName(table.getSchemaName(), table.getTableName());
        write2Csv(dataList, csvPath);
        csvPathList.add(csvPath);
        return csvPathList;
    }

    private void lockTable(Table table, Statement statement) throws SQLException {
        String schemaName = table.getSchemaName();
        String tableName = table.getTableName();
        String lockStatement = String.format("BEGIN; LOCK TABLE %s.%s IN SHARE MODE;", schemaName, tableName);
        statement.execute(lockStatement);
        statement.execute(getLockSql(schemaName, tableName));
    }

    private Optional<Column> readTableColumn(ResultSet columnMetadata) throws SQLException {
        Column column = Column.builder()
            .name(columnMetadata.getString(4))
            .jdbcType(columnMetadata.getInt(5))
            .typeName(columnMetadata.getString(6))
            .length(columnMetadata.getInt(7))
            .optional(isNullable(columnMetadata.getInt(11)))
            .position(columnMetadata.getInt(17))
            .autoIncremented("YES".equalsIgnoreCase(columnMetadata.getString(23)))
            .defaultValueExpression(columnMetadata.getString(13))
            .build();
        if (columnMetadata.getObject(9) != null) {
            column.setScale(columnMetadata.getInt(9));
        }
        String autogenerated = null;
        try {
            autogenerated = columnMetadata.getString(24);
        } catch (SQLException e) {
            // ignore, some drivers don't have this index - e.g. Postgres
        }
        column.setGenerated("YES".equalsIgnoreCase(autogenerated));
        return Optional.of(column);
    }

    private List<String> getDataList(ResultSet rs, int columnSize) throws SQLException {
        List<String> dataList = new ArrayList<>();
        while (rs.next()) {
            // rowToArray
            final Object[] rowArr = new Object[columnSize];
            for (int i = 0; i < columnSize; i++) {
                rowArr[i] = rs.getString(i + 1);
            }
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < columnSize; i++) {
                Object value = rowArr[i];
                if (value instanceof ByteBuffer) {
                    ByteBuffer object = (ByteBuffer) value;
                    value = new String(object.array(), object.position(), object.limit(), Charset.defaultCharset());
                }
                if (value instanceof byte[]) {
                    StringBuilder bytes = new StringBuilder();
                    byte[] obj = (byte[]) value;
                    if (obj.length > 0) {
                        bytes.append("\\x");
                    }
                    bytes.append(HexConverter.convertToHexString(obj));
                    value = bytes.toString();
                }
                if (value != null) {
                    sb.append("\"")
                        .append(value.toString().replace("\"", "\"\""))
                        .append("\"")
                        .append(CommonConstants.DELIMITER);
                } else {
                    sb.append(value).append(CommonConstants.DELIMITER);
                }
                if (i == columnSize - 1) {
                    sb.deleteCharAt(sb.length() - 1);
                }
            }
            dataList.add(sb.toString());
        }
        return dataList;
    }

    private boolean isNullable(int jdbcNullable) {
        return jdbcNullable == ResultSetMetaData.columnNullable
            || jdbcNullable == ResultSetMetaData.columnNullableUnknown;
    }

    private String getCsvFileName(String schema, String table) {
        return new File(sourceConfig.getCsvDir()) + File.separator + String.format(Locale.ROOT, "%s_%s.csv", schema,
            table);
    }

    private void write2Csv(List<String> dataList, String path) {
        if (dataList.isEmpty()) {
            LOGGER.warn("");
        }
        File file = new File(path);
        try (FileOutputStream fileInputStream = new FileOutputStream(file);
            PrintWriter printWriter = new PrintWriter(fileInputStream, true);) {
            String data = String.join(System.lineSeparator(), dataList) + System.lineSeparator();
            printWriter.write(data);
            printWriter.flush();
            LOGGER.info("success to write data to csv file {}", path);
        } catch (IOException e) {
            LOGGER.error("");
        }
    }

    /**
     * readObjects
     *
     * @param objectType objectType
     */
    public void readObjects(String objectType) {
        String querySql = getQueryObjectSql(objectType.toLowerCase(Locale.ROOT));
        try (Connection conn = connection.getConnection(sourceConfig.getDbConn());
            Statement statement = conn.createStatement();
            ResultSet rs = statement.executeQuery(querySql);) {
            while (rs.next()) {
                DbObject dbObject = new DbObject();
                dbObject.setName(rs.getString("name"));
                dbObject.setDefinition(convertDefinition(rs.getString("definition")));
                LOGGER.info("{}:{}", objectType, dbObject.getDefinition());
                QueueManager.getInstance().addQueue(dbObject);
            }
        } catch (SQLException | InterruptedException e) {
            LOGGER.error("fail to read views, errorMsg:{}", e.getMessage());
        }
    }
}
