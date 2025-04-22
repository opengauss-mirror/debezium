/*
 * Copyright (c) 2025-2025 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *           http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package org.full.migration.source.service;

import org.apache.commons.lang3.StringUtils;
import org.full.migration.constants.CommonConstants;
import org.full.migration.coordinator.QueueManager;
import org.full.migration.model.config.SourceConfig;
import org.full.migration.model.table.Column;
import org.full.migration.model.table.PartitionDefinition;
import org.full.migration.model.table.SliceInfo;
import org.full.migration.model.table.Table;
import org.full.migration.model.table.TableData;
import org.full.migration.translator.SqlServerColumnType;
import org.full.migration.translator.SqlServerFuncTranslator;
import org.full.migration.utils.FileUtils;
import org.full.migration.utils.HexConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.StringJoiner;
import java.util.stream.Collectors;

/**
 * TableService
 *
 * @since 2025-04-18
 */
public class SourceTableService {
    private static final Logger LOGGER = LoggerFactory.getLogger(SourceTableService.class);

    private final SourceConfig sourceConfig;

    /**
     * Constructor
     *
     * @param sourceConfig sourceConfig
     */
    public SourceTableService(SourceConfig sourceConfig) {
        this.sourceConfig = sourceConfig;
    }

    /**
     * isSkipTable
     *
     * @param schema schema
     * @param tableName tableName
     * @return isSkipTable
     */
    public boolean isSkipTable(String schema, String tableName) {
        List<String> limitTables = sourceConfig.getLimitTables();
        List<String> skipTables = sourceConfig.getSkipTables();
        String fullTableName = (schema + "." + tableName).toLowerCase(Locale.ROOT);
        boolean isLimitEmpty = CollectionUtils.isEmpty(limitTables);
        if (!isLimitEmpty) {
            return limitTables.stream().map(name -> name.toLowerCase(Locale.ROOT)).noneMatch(fullTableName::equals);
        }
        boolean isSkipEmpty = CollectionUtils.isEmpty(skipTables);
        if (!isSkipEmpty) {
            return skipTables.stream().map(name -> name.toLowerCase(Locale.ROOT)).anyMatch(fullTableName::equals);
        }
        return false;
    }

    /**
     * getPartitionDdl
     *
     * @param partitionDef partitionDef
     * @return partitionDdl
     */
    public String getPartitionDdl(PartitionDefinition partitionDef) {
        StringBuilder partitionDdl = new StringBuilder("\n PARTITION BY ").append(partitionDef.getPartitionType())
            .append(" (")
            .append(partitionDef.getPartitionColumn())
            .append(")");
        if (partitionDef.isRangePartition()) {
            partitionDdl.append(generateRangePartitionDdl(partitionDef));
        } else if (partitionDef.isListPartition()) {
            partitionDdl.append(generateListPartitionDdl(partitionDef));
        } else if (partitionDef.isHashPartition()) {
            partitionDdl.append(generateHashPartitionDdl(partitionDef));
        } else {
            return null;
        }
        return partitionDdl.toString();
    }

    private String generateRangePartitionDdl(PartitionDefinition partitionDef) {
        StringBuilder rangePartitionDdl = new StringBuilder();
        rangePartitionDdl.append(" (");
        List<String> boundaries = partitionDef.getBoundaries();
        for (int i = 0; i <= boundaries.size(); i++) {
            String partitionName = "p" + i;
            String condition;
            if (i == boundaries.size()) {
                condition = "VALUES LESS THAN (MAXVALUE)";
            } else {
                condition = "VALUES LESS THAN ('" + boundaries.get(i) + "')";
            }
            rangePartitionDdl.append("\n  PARTITION ").append(partitionName).append(" ").append(condition).append(",");
        }
        rangePartitionDdl.deleteCharAt(rangePartitionDdl.length() - 1);
        rangePartitionDdl.append("\n)");
        return rangePartitionDdl.toString();
    }

    private String generateListPartitionDdl(PartitionDefinition partitionDef) {
        StringBuilder listPartitionDdl = new StringBuilder();
        listPartitionDdl.append(" (");
        List<String> boundaries = partitionDef.getBoundaries();
        for (int i = 0; i < boundaries.size(); i++) {
            String partitionName = "p" + i;
            listPartitionDdl.append("\n  PARTITION ")
                .append(partitionName)
                .append(" VALUES (")
                .append(boundaries.get(i))
                .append("),");
        }
        // 添加默认分区
        listPartitionDdl.append("\n  PARTITION p_default VALUES (DEFAULT)");
        listPartitionDdl.append("\n)");
        return listPartitionDdl.toString();
    }

    private String generateHashPartitionDdl(PartitionDefinition partitionDef) {
        StringBuilder hashPartitionDdl = new StringBuilder();
        hashPartitionDdl.append(" (");
        int partitionCount = partitionDef.getPartitionCount();
        for (int i = 0; i < partitionCount; i++) {
            hashPartitionDdl.append("\n  PARTITION p").append(i).append(",");
        }
        hashPartitionDdl.deleteCharAt(hashPartitionDdl.length() - 1);
        hashPartitionDdl.append("\n)");
        return hashPartitionDdl.toString();
    }

    /**
     * getCreateTableSql
     *
     * @param table table
     * @param columns columns
     * @param partitionDdl partitionDdl
     * @return sql for creating table
     */
    public Optional<String> getCreateTableSql(Table table, List<Column> columns, String partitionDdl) {
        StringJoiner columnDdl = new StringJoiner(", ");
        for (Column column : columns) {
            String colName = column.getName();
            String colType = getColTypeStr(column);
            if (SqlServerColumnType.isTimesTypes(colType) && !sourceConfig.getIsTimeMigrate()) {
                LOGGER.error("{}.{} has column type {}, don't migrate this table according to the configuration",
                    table.getSchemaName(), table.getTableName(), colType);
                return Optional.empty();
            }
            String nullType = column.isOptional() ? "" : " NOT NULL ";
            columnDdl.add(String.format("%s %s %s", colName, colType, nullType));
        }
        String defCols = columnDdl.toString();
        String sql = String.format("CREATE TABLE if not exists %s.%s ( %s ) %s", table.getTargetSchemaName(),
            table.getTableName(), defCols, partitionDdl == null ? "" : partitionDdl);
        return Optional.ofNullable(sql);
    }

    private String getColTypeStr(Column column) {
        if (column.isAutoIncremented()) {
            return "serial";
        }
        String typeName = column.getTypeName().split(" ")[0];
        String ogType = SqlServerColumnType.convertType(typeName);
        StringBuilder builder = new StringBuilder(ogType);
        if (SqlServerColumnType.isTypeWithLength(typeName)) {
            long length = column.getLength();
            Integer scale = column.getScale();
            // 大文本类型varchar(max),nvarchar(max),varbinary(max)
            if ((SqlServerColumnType.isVarsTypes(typeName) || SqlServerColumnType.isBinaryTypes(typeName))
                && length == Integer.MAX_VALUE) {
                return SqlServerColumnType.convertType(typeName + "(max)");
            }
            if (SqlServerColumnType.isTimesTypes(typeName)) {
                if (!sourceConfig.getIsTimeMigrate()) {
                    return typeName;
                }
                builder.append("(").append(scale > 6 ? 6 : scale).append(")");
                if (SqlServerColumnType.SS_DATETIMEOFFSET.getSsType().equals(typeName)) {
                    builder.append(" with time zone ");
                }
            } else {
                // 可变长类型length == Integer.MAX_VALUE时表示没指定长度, numeric类型length==0时没指定长度和精度。
                if (hasLengthLimit(typeName, length)) {
                    builder.append("(").append(length);
                }
                // numeric类型获取scale
                if (SqlServerColumnType.isNumericType(typeName) && length > 0 && scale != null && scale > 0) {
                    builder.append(",").append(scale);
                }
                if (hasLengthLimit(typeName, length)) {
                    builder.append(")");
                }
            }
        }
        String defaultValue = column.getDefaultValueExpression();
        if (StringUtils.isNoneEmpty(defaultValue)) {
            builder.append(" default ").append(SqlServerFuncTranslator.convertDefinition(defaultValue));
        }
        return builder.toString();
    }

    private static boolean hasLengthLimit(String typeName, long length) {
        return (SqlServerColumnType.isVarsTypes(typeName) && length != Integer.MAX_VALUE) || (
            SqlServerColumnType.isNumericType(typeName) && length > 0) || (!SqlServerColumnType.isVarsTypes(typeName)
            && !SqlServerColumnType.isNumericType(typeName) && !SqlServerColumnType.isBinaryTypes(typeName));
    }

    /**
     * exportResultSetToCsv
     *
     * @param rs rs
     * @param table table
     * @param columns columns
     * @param pageRows pageRows
     * @param snapshotPoint snapshotPoint
     * @throws SQLException SQLException
     * @throws IOException IOException
     */
    public void exportResultSetToCsv(ResultSet rs, Table table, List<Column> columns, int pageRows,
        String snapshotPoint) throws SQLException, IOException {
        String tableCsvPath = sourceConfig.getCsvDir();
        FileUtils.createDir(tableCsvPath);
        int fileIndex = 1;
        BufferedWriter writer = initializeWriter(table, tableCsvPath, fileIndex, columns);
        try {
            int rowCount = 0;
            long totalSlice = table.getRowCount() / pageRows + 1;
            while (rs.next()) {
                rowCount++;
                String line = formatData(rs, columns);
                writer.write(line);
                writer.newLine();
                if (rowCount % pageRows == 0) {
                    writer.flush();
                    writer.close();
                    SliceInfo sliceInfo = new SliceInfo(fileIndex, totalSlice, pageRows, false);
                    TableData tableData = new TableData(table,
                        FileUtils.getCurrentFilePath(table, tableCsvPath, fileIndex), snapshotPoint, sliceInfo);
                    updateTableDataQueue(tableData);
                    fileIndex++;
                    writer = initializeWriter(table, tableCsvPath, fileIndex, columns);
                    rowCount = 0;
                }
            }
            TableData tableData = new TableData(table, FileUtils.getCurrentFilePath(table, tableCsvPath, fileIndex),
                snapshotPoint, new SliceInfo(fileIndex, totalSlice, rowCount, true));
            if (rowCount == 0) {
                String filePath = FileUtils.getCurrentFilePath(table, tableCsvPath, fileIndex - 1);
                SliceInfo sliceInfo = new SliceInfo(fileIndex - 1, totalSlice, pageRows, true);
                tableData.setDataPath(filePath);
                tableData.setSliceInfo(sliceInfo);
                updateTableDataQueue(tableData);
            } else {
                writer.flush();
                writer.close();
                updateTableDataQueue(tableData);
            }
        } catch (IOException e) {
            LOGGER.error("write csv file has occurred an IOException, error message:{}", e.getMessage());
        } finally {
            if (writer != null) {
                writer.close();
            }
        }
        LOGGER.info("finished to read table:{}, generate {} csv files", table.getTableName(), fileIndex);
    }

    private BufferedWriter initializeWriter(Table table, String tableCsvPath, int fileIndex, List<Column> columns)
        throws IOException {
        BufferedWriter writer = FileUtils.createNewFileWriter(table, tableCsvPath, fileIndex);
        String header = formatHeader(columns);
        writer.write(header);
        writer.newLine();
        return writer;
    }

    private void updateTableDataQueue(TableData tableData) {
        QueueManager.getInstance().putToQueue(QueueManager.TABLE_DATA_QUEUE, tableData);
    }

    private String formatHeader(List<Column> columns) {
        return columns.stream().map(Column::getName).collect(Collectors.joining(CommonConstants.DELIMITER));
    }

    private String formatData(ResultSet rs, List<Column> columns) throws SQLException {
        int columnCount = columns.size();
        final List<Object> rowList = new ArrayList<>(columnCount);
        for (int i = 0; i < columnCount; i++) {
            rowList.add(rs.getObject(i + 1));
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < columnCount; i++) {
            Object value = rowList.get(i);
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
            if (i == columnCount - 1) {
                sb.deleteCharAt(sb.length() - 1);
            }
        }
        return sb.toString();
    }

    /**
     * readTableColumn
     *
     * @param columnMetadata columnMetadata
     * @return Column
     * @throws SQLException SQLException
     */
    public Optional<Column> readTableColumn(ResultSet columnMetadata) throws SQLException {
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

    private boolean isNullable(int jdbcNullable) {
        return jdbcNullable == ResultSetMetaData.columnNullable
            || jdbcNullable == ResultSetMetaData.columnNullableUnknown;
    }
}
