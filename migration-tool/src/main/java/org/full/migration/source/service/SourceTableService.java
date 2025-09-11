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

import org.apache.commons.collections4.CollectionUtils;
import org.full.migration.model.config.SourceConfig;
import org.full.migration.model.table.Column;
import org.full.migration.model.table.PartitionDefinition;
import org.full.migration.model.table.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.ResultSetMetaData;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

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

    public boolean migSegmentState(String schema, String tableName) {
        List<String> segmentTables = sourceConfig.getAddSegmentTables();
        String fullTableName = (schema + "." + tableName).toLowerCase(Locale.ROOT);
        return segmentTables != null && segmentTables.stream()
                .map(name -> name.toLowerCase(Locale.ROOT))
                .anyMatch(fullTableName::equals);
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
     * @param columnDdl columnDdl
     * @param partitionDdl partitionDdl
     * @return sql for creating table
     */
    public Optional<String> getCreateTableSql(Table table, String columnDdl, String partitionDdl, String inheritsDdl) {
        String segmentDdl = "";
        if (table.isHasSegment() || migSegmentState(table.getSchemaName(), table.getTableName())) {
            segmentDdl = "with(segment = on)";
        }

        StringBuilder sqlBuilder = new StringBuilder("CREATE TABLE IF NOT EXISTS ")
                .append(table.getTargetSchemaName())
                .append(".")
                .append(table.getTableName())
                .append("(")
                .append(columnDdl)
                .append(")")
                .append(segmentDdl)
                .append("\n")
                .append(partitionDdl == null ? "" : partitionDdl)
                .append("\n")
                .append(inheritsDdl == null ? "" : inheritsDdl);

        return Optional.of(sqlBuilder.toString().replaceAll("\\s+", " ").trim());
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
