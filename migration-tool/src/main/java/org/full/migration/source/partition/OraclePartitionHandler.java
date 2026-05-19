/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */

package org.full.migration.source.partition;

import org.full.migration.constants.OracleSqlConstants;
import org.full.migration.model.table.postgres.partition.HashPartitionInfo;
import org.full.migration.model.table.postgres.partition.ListPartitionInfo;
import org.full.migration.model.table.postgres.partition.PartitionInfo;
import org.full.migration.model.table.postgres.partition.RangePartitionInfo;
import org.full.migration.translator.Source2TargetTranslator;
import org.full.migration.translator.TranslatorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Oracle 分区处理器实现
 *
 * @since 2025-04-18
 */
public class OraclePartitionHandler implements PartitionHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(OraclePartitionHandler.class);
    private final Source2TargetTranslator translator;

    public OraclePartitionHandler() {
        // Default translator: Oracle to OGRAC
        this.translator = TranslatorFactory.getTranslator("oracle", "ograc");
    }

    public OraclePartitionHandler(Source2TargetTranslator translator) {
        this.translator = translator;
    }

    @Override
    public PartitionMetadata getPartitionMetadata(String schema, String tableName, Connection conn) throws SQLException {
        PartitionMetadata metadata = new PartitionMetadata();
        queryBasicPartitionMetadata(tableName, conn, metadata);
        if (metadata.isPartitioned()) {
            List<String> partitionColumns = queryPartitionColumns(tableName, conn);
            buildPartitionKey(metadata, partitionColumns);
        }
        if (metadata.isSubPartitioned()) {
            List<String> subPartitionColumns = querySubPartitionColumns(tableName, conn);
            buildSubPartitionKey(metadata, subPartitionColumns);
        }
        return metadata;
    }
    
    /**
     * Query basic partition metadata
     *
     * @param tableName Table name
     * @param conn Database connection
     * @param metadata Partition metadata
     * @throws SQLException SQL exception
     */
    private void queryBasicPartitionMetadata(String tableName, Connection conn, PartitionMetadata metadata) throws SQLException {
        try (PreparedStatement pstmt = conn.prepareStatement(OracleSqlConstants.QUERY_PARTITION_METADATA_SQL)) {
            pstmt.setString(1, tableName.toUpperCase(Locale.ROOT));
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    boolean isPartitioned = rs.getBoolean("PARTITIONED");
                    metadata.setPartitioned(isPartitioned);
                    
                    if (isPartitioned) {
                        String partitioningType = rs.getString("PARTITIONING_TYPE");
                        metadata.setPartitioningType(partitioningType);
                        if (partitioningType == null || partitioningType.isEmpty()) {
                            metadata.setPartitioningType(PartitionInfo.RANGE_PARTITION);
                        }
                        String interval = rs.getString("INTERVAL");
                        if (interval != null && !interval.isEmpty()) {
                            metadata.setIntervalDefinition(interval);
                        }
                        String subpartitioned = rs.getString("subpartitioned");
                        if ("YES".equals(subpartitioned)) {
                            metadata.setSubPartitioned(true);
                            String subPartitioningType = rs.getString("subpartitioning_type");
                            metadata.setSubPartitioningType(subPartitioningType);
                        }
                    }
                }
            }
        }
    }
    
    /**
     * Query partition columns
     *
     * @param tableName Table name
     * @param conn Database connection
     * @return List of partition columns
     * @throws SQLException SQL exception
     */
    private List<String> queryPartitionColumns(String tableName, Connection conn) throws SQLException {
        List<String> partitionColumns = new ArrayList<>();
        try (PreparedStatement pstmt = conn.prepareStatement(OracleSqlConstants.QUERY_PARTITION_COLUMNS_SQL)) {
            pstmt.setString(1, tableName.toUpperCase(Locale.ROOT));
            try (ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    partitionColumns.add(rs.getString("column_name"));
                }
            }
        }
        return partitionColumns;
    }
    
    /**
     * Query subpartition columns
     *
     * @param tableName Table name
     * @param conn Database connection
     * @return List of subpartition columns
     * @throws SQLException SQL exception
     */
    private List<String> querySubPartitionColumns(String tableName, Connection conn) throws SQLException {
        List<String> subPartitionColumns = new ArrayList<>();
        try (PreparedStatement pstmt = conn.prepareStatement(OracleSqlConstants.QUERY_SUBPARTITION_COLUMNS_SQL)) {
            pstmt.setString(1, tableName.toUpperCase(Locale.ROOT));
            try (ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    subPartitionColumns.add(rs.getString("column_name"));
                }
            }
        }
        return subPartitionColumns;
    }
    
    /**
     * Build partition key
     *
     * @param metadata Partition metadata
     * @param partitionColumns List of partition columns
     */
    private void buildPartitionKey(PartitionMetadata metadata, List<String> partitionColumns) {
        if (!partitionColumns.isEmpty()) {
            String partitioningType = metadata.getPartitioningType();
            if (partitioningType == null || partitioningType.isEmpty()) {
                partitioningType = PartitionInfo.RANGE_PARTITION;
            }
            String partitionKey = partitioningType + " (" + String.join(", ", partitionColumns) + ")";
            metadata.setPartitionKey(partitionKey);
        }
    }
    
    /**
     * Build subpartition key
     *
     * @param metadata Partition metadata
     * @param subPartitionColumns List of subpartition columns
     */
    private void buildSubPartitionKey(PartitionMetadata metadata, List<String> subPartitionColumns) {
        if (!subPartitionColumns.isEmpty()) {
            String subPartitioningType = metadata.getSubPartitioningType();
            if (subPartitioningType == null || subPartitioningType.isEmpty()) {
                subPartitioningType = "HASH";
            }
            String subPartitionKey = subPartitioningType + " (" + String.join(", ", subPartitionColumns) + ")";
            metadata.setSubPartitionKey(subPartitionKey);
        }
    }

    @Override
    public List<PartitionInfo> getPartitionInfos(String schema, String tableName, String partitionName, Connection conn) throws SQLException {
        List<PartitionInfo> partitionInfos = new ArrayList<>();
        PartitionMetadata metadata = getPartitionMetadata(schema, tableName, conn);
        String partitionType = metadata.getPartitioningType();
        if (partitionType == null || partitionType.isEmpty()) {
            partitionType = PartitionInfo.RANGE_PARTITION;
        }
        
        if (partitionName == null) {
            try (PreparedStatement pstmt = conn.prepareStatement(OracleSqlConstants.QUERY_PARTITION_KEY_SQL)) {
                pstmt.setString(1, tableName.toUpperCase(Locale.ROOT));
                try (ResultSet rs = pstmt.executeQuery()) {
                    while (rs.next()) {
                        PartitionInfo partitionInfo = createPartitionInfo(partitionType, rs);
                        partitionInfo.setPartitionTable(rs.getString("partition_name"));
                        partitionInfo.setParentTable(tableName);
                        partitionInfo.setTablespaceName(rs.getString("tablespace_name"));
                        partitionInfos.add(partitionInfo);
                    }
                }
            }
        } else {
            try (PreparedStatement pstmt = conn.prepareStatement(OracleSqlConstants.QUERY_SUBPARTITIONS_SQL)) {
                pstmt.setString(1, tableName.toUpperCase(Locale.ROOT));
                pstmt.setString(2, partitionName.toUpperCase(Locale.ROOT));
                try (ResultSet rs = pstmt.executeQuery()) {
                    while (rs.next()) {
                        String subPartitionType = metadata.getSubPartitioningType();
                        if (subPartitionType == null || subPartitionType.isEmpty()) {
                            subPartitionType = partitionType;
                        }
                        PartitionInfo partitionInfo = createPartitionInfo(subPartitionType, rs);
                        partitionInfo.setPartitionTable(rs.getString("subpartition_name"));
                        partitionInfo.setParentTable(partitionName);
                        partitionInfo.setTablespaceName(rs.getString("tablespace_name"));
                        partitionInfos.add(partitionInfo);
                    }
                }
            }
        }
        return partitionInfos;
    }

    @Override
    public String extractSubPartitionKey(String schema, String tableName, Connection conn) throws SQLException {
        PartitionMetadata metadata = getPartitionMetadata(schema, tableName, conn);
        return metadata.getSubPartitionKey();
    }

    @Override
    public String getRangeUpperBound(PartitionInfo partition) {
        String highValue = null;
        if (partition instanceof RangePartitionInfo) {
            highValue = ((RangePartitionInfo) partition).getRangeUpperBound();
        } else {
            highValue = partition.getRangeUpperBound();
        }
        if (highValue != null && !highValue.isEmpty()) {
            return translator.translatePartitionFunction(highValue, false).orElse(highValue);
        }
        return "MAXVALUE";
    }

    @Override
    public String getListPartitionValue(PartitionInfo partition) {
        String value = null;
        if (partition instanceof ListPartitionInfo) {
            value = ((ListPartitionInfo) partition).getListPartitionValue();
        } else {
            value = partition.getListPartitionValue();
        }
        if (value != null && !value.isEmpty()) {
            return translator.translatePartitionFunction(value, false).orElse(value);
        }
        return value;
    }

    /**
     * Create partition info based on partition type
     *
     * @param partitionType Partition type
     * @param rs Result set
     * @return Partition info
     * @throws SQLException SQL exception
     */
    private PartitionInfo createPartitionInfo(String partitionType, ResultSet rs) throws SQLException {
        PartitionInfo partitionInfo;
        String highValue = rs.getString("high_value");
        if (partitionType.startsWith(PartitionInfo.RANGE_PARTITION)) {
            RangePartitionInfo rangePartitionInfo = new RangePartitionInfo();
            rangePartitionInfo.setRangeUpperBound(highValue);
            partitionInfo = rangePartitionInfo;
        } else if (partitionType.startsWith(PartitionInfo.LIST_PARTITION)) {
            ListPartitionInfo listPartitionInfo = new ListPartitionInfo();
            listPartitionInfo.setListPartitionValue(highValue);
            partitionInfo = listPartitionInfo;
        } else if (partitionType.startsWith(PartitionInfo.HASH_PARTITION)) {
            HashPartitionInfo hashPartitionInfo = new HashPartitionInfo();
            hashPartitionInfo.setHashPartitionValue(highValue);
            partitionInfo = hashPartitionInfo;
        } else {
            RangePartitionInfo rangePartitionInfo = new RangePartitionInfo();
            rangePartitionInfo.setRangeUpperBound(highValue);
            partitionInfo = rangePartitionInfo;
        }
        return partitionInfo;
    }
    

    @Override
    public String generatePartitionDdl(Connection conn, String schema, String tableName, boolean isSubPartition) throws SQLException {
        PartitionMetadata metadata = getPartitionMetadata(schema, tableName, conn);
        if (!metadata.isPartitioned() || metadata.getPartitionKey() == null) {
            return "";
        }

        String partitionKey = metadata.getPartitionKey();
        if (metadata.isSubPartitioned() && metadata.getSubPartitionKey() != null) {
            List<PartitionInfo> partitions = getPartitionInfos(schema, tableName, null, conn);
            java.util.Map<String, List<PartitionInfo>> subPartitionsMap = new java.util.HashMap<>();
            for (PartitionInfo partition : partitions) {
                List<PartitionInfo> subPartitions = getPartitionInfos(schema, tableName, partition.getPartitionTable(), conn);
                if (!subPartitions.isEmpty()) {
                    subPartitionsMap.put(partition.getPartitionTable(), subPartitions);
                }
            }
            
            String partitionDdl = "PARTITION BY " + partitionKey + " ";
            PartitionHandler.PartitionDdlContext context = new PartitionHandler.PartitionDdlContext();
            context.setSchema(schema);
            context.setTableName(tableName);
            context.setConn(conn);
            context.setPartitionKey(partitionKey);
            context.setPartitions(partitions);
            context.setSubPartitionsMap(subPartitionsMap);
            return partitionDdl + generateCompositePartitionDdl(context);
        } else {
            List<PartitionInfo> partitions = getPartitionInfos(schema, tableName, null, conn);
            if (metadata.getIntervalDefinition() != null) {
                String partitionDdl = "PARTITION BY " + partitionKey + " ";
                return partitionDdl + generateIntervalPartitionDdl(partitions, metadata.getIntervalDefinition());
            } else {
                String partitionDdl = "PARTITION BY " + partitionKey + " ";
                PartitionHandler.PartitionDdlContext context = new PartitionHandler.PartitionDdlContext();
                context.setSchema(schema);
                context.setTableName(tableName);
                context.setConn(conn);
                context.setPartitionKey(partitionKey);
                context.setPartitions(partitions);
                context.setSubPartitionsMap(null);
                return partitionDdl + generatePartitionTypeDdl(context);
            }
        }
    }
}
