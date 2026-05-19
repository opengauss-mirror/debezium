/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */

package org.full.migration.source.partition;

import org.full.migration.model.table.postgres.partition.PartitionInfo;

import lombok.Data;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 * Partition handler interface
 *
 * @since 2025-04-18
 */
public interface PartitionHandler {

    /**
     * Get partition metadata
     *
     * @param schema    Schema name
     * @param tableName Table name
     * @param conn      Database connection
     * @return Partition metadata
     * @throws SQLException SQL exception
     */
    PartitionMetadata getPartitionMetadata(String schema, String tableName, Connection conn) throws SQLException;

    /**
     * Get partition information
     *
     * @param schema        Schema name
     * @param tableName     Table name
     * @param partitionName Partition name (null for top-level partitions)
     * @param conn          Database connection
     * @return List of partition information
     * @throws SQLException SQL exception
     */
    List<PartitionInfo> getPartitionInfos(String schema, String tableName, String partitionName, Connection conn)
            throws SQLException;

    /**
     * Generate partition DDL
     *
     * @param conn           Database connection
     * @param schema         Schema name
     * @param tableName      Table name
     * @param isSubPartition Whether it is a subpartition
     * @return Partition DDL
     * @throws SQLException SQL exception
     */
    String generatePartitionDdl(Connection conn, String schema, String tableName, boolean isSubPartition)
            throws SQLException;

    /**
     * Generate DDL for a specific partition type
     *
     * @param context Partition DDL context
     * @return Partition DDL
     * @throws SQLException SQL exception
     */
    default String generatePartitionTypeDdl(PartitionDdlContext context) throws SQLException {
        if (context.getSubPartitionsMap() != null && !context.getSubPartitionsMap().isEmpty()) {
            return generateCompositePartitionDdl(context);
        } else {
            if (context.getPartitionKey().startsWith("RANGE")) {
                return generateRangePartitionDdl(context.getPartitions());
            } else if (context.getPartitionKey().startsWith("LIST")) {
                return generateListPartitionDdl(context.getPartitions());
            } else if (context.getPartitionKey().startsWith("HASH")) {
                return generateHashPartitionDdl(context.getPartitions());
            } else {
                throw new IllegalArgumentException("Unknown partition type: " + context.getPartitionKey());
            }
        }
    }

    /**
     * Generate range partition DDL
     *
     * @param partitions List of partition information
     * @return Range partition DDL
     */
    default String generateRangePartitionDdl(List<PartitionInfo> partitions) {
        StringBuilder ddl = new StringBuilder("(");
        for (int i = 0; i < partitions.size(); i++) {
            PartitionInfo partition = partitions.get(i);
            String highValue = getRangeUpperBound(partition);
            ddl.append("PARTITION ").append(partition.getPartitionTable())
                    .append(" VALUES LESS THAN (").append(highValue).append(")");
            ddl.append(generatePartitionTablespaceName(partition));
            if (i < partitions.size() - 1) {
                ddl.append(", ");
            }
        }
        ddl.append(")");
        return ddl.toString();
    }

    /**
     * Generate tablespace clause for a partition
     *
     * @param partition Partition information
     * @return Tablespace clause
     */
    private String generatePartitionTablespaceName(PartitionInfo partition) {
        return (partition.getTablespaceName() != null && !partition.getTablespaceName().isEmpty()) ? " TABLESPACE " + partition.getTablespaceName() : "";
    }
    /**
     * Generate list partition DDL
     *
     * @param partitions List of partition information
     * @return List partition DDL
     */
    default String generateListPartitionDdl(List<PartitionInfo> partitions) {
        StringBuilder ddl = new StringBuilder("(");
        for (int i = 0; i < partitions.size(); i++) {
            PartitionInfo partition = partitions.get(i);
            String value = getListPartitionValue(partition);
            ddl.append("PARTITION ").append(partition.getPartitionTable())
                    .append(" VALUES (").append(value).append(")");
            ddl.append(generatePartitionTablespaceName(partition));
            if (i < partitions.size() - 1) {
                ddl.append(", ");
            }
        }
        ddl.append(")");
        return ddl.toString();
    }

    /**
     * Generate hash partition DDL
     *
     * @param partitions List of partition information
     * @return Hash partition DDL
     */
    default String generateHashPartitionDdl(List<PartitionInfo> partitions) {
        StringBuilder ddl = new StringBuilder("(");
        for (int i = 0; i < partitions.size(); i++) {
            PartitionInfo partition = partitions.get(i);
            ddl.append("PARTITION ").append(partition.getPartitionTable());
            ddl.append(generatePartitionTablespaceName(partition));
            if (i < partitions.size() - 1) {
                ddl.append(", ");
            }
        }
        ddl.append(")");
        return ddl.toString();
    }

    /**
     * Generate interval partition DDL
     *
     * @param partitions         List of partition information
     * @param intervalDefinition Interval definition (e.g., NUMTOYMINTERVAL(1,
     *                           'MONTH'))
     * @return Interval partition DDL
     */
    default String generateIntervalPartitionDdl(List<PartitionInfo> partitions, String intervalDefinition) {
        StringBuilder ddl = new StringBuilder("INTERVAL (").append(intervalDefinition).append(") (");
        if (!partitions.isEmpty()) {
            PartitionInfo partition = partitions.get(0);
            String partitionName = partition.getPartitionTable();
            if (partitionName != null && !partitionName.startsWith("SYS_")) {
                String highValue = getRangeUpperBound(partition);
                ddl.append("PARTITION ").append(partitionName)
                        .append(" VALUES LESS THAN (").append(highValue).append(")");
            }
        }
        ddl.append(")");
        return ddl.toString();
    }

    /**
     * Generate composite partition DDL
     *
     * @param context Partition DDL context
     * @return Composite partition DDL
     * @throws SQLException SQL exception
     */
    default String generateCompositePartitionDdl(PartitionDdlContext context) throws SQLException {
        StringBuilder ddl = new StringBuilder();
        String subPartitionKey = extractSubPartitionKey(context.getSchema(), context.getTableName(), context.getConn());
        appendSubPartitionClause(ddl, subPartitionKey);

        for (int i = 0; i < context.getPartitions().size(); i++) {
            PartitionInfo partition = context.getPartitions().get(i);
            appendPartitionDefinition(ddl, context.getPartitionKey(), partition);

            List<PartitionInfo> subPartitions = context.getSubPartitionsMap().get(partition.getPartitionTable());
            if (subPartitions != null && !subPartitions.isEmpty()) {
                appendSubPartitions(ddl, subPartitionKey, subPartitions);
            }

            if (i < context.getPartitions().size() - 1) {
                ddl.append(", ");
            }
        }

        ddl.append(")");
        return ddl.toString();
    }

    /**
     * Append subpartition clause to DDL
     *
     * @param ddl             StringBuilder for DDL
     * @param subPartitionKey Subpartition key
     */
    default void appendSubPartitionClause(StringBuilder ddl, String subPartitionKey) {
        if (subPartitionKey != null) {
            ddl.append("SUBPARTITION BY ").append(subPartitionKey).append(" (");
        } else {
            ddl.append("(");
        }
    }

    /**
     * Append partition definition to DDL
     *
     * @param ddl          StringBuilder for DDL
     * @param partitionKey Partition key
     * @param partition    Partition information
     */
    default void appendPartitionDefinition(StringBuilder ddl, String partitionKey, PartitionInfo partition) {
        ddl.append("PARTITION ").append(partition.getPartitionTable());
        if (partitionKey.startsWith("RANGE")) {
            String highValue = getRangeUpperBound(partition);
            ddl.append(" VALUES LESS THAN (").append(highValue).append(")");
        } else if (partitionKey.startsWith("LIST")) {
            String value = getListPartitionValue(partition);
            ddl.append(" VALUES (").append(value).append(")");
        }
    }

    /**
     * Append subpartitions to DDL
     *
     * @param ddl             StringBuilder for DDL
     * @param subPartitionKey Subpartition key
     * @param subPartitions   List of subpartition information
     */
    default void appendSubPartitions(StringBuilder ddl, String subPartitionKey, List<PartitionInfo> subPartitions) {
        ddl.append(" (");
        for (int j = 0; j < subPartitions.size(); j++) {
            PartitionInfo subPartition = subPartitions.get(j);
            ddl.append("SUBPARTITION ").append(subPartition.getPartitionTable());
            if (subPartitionKey != null) {
                appendSubPartitionValues(ddl, subPartitionKey, subPartition);
            }
            if (j < subPartitions.size() - 1) {
                ddl.append(", ");
            }
        }
        ddl.append(")");
    }

    /**
     * Append subpartition values to DDL
     *
     * @param ddl             StringBuilder for DDL
     * @param subPartitionKey Subpartition key
     * @param subPartition    Subpartition information
     */
    default void appendSubPartitionValues(StringBuilder ddl, String subPartitionKey, PartitionInfo subPartition) {
        if (subPartitionKey.startsWith("RANGE")) {
            String highValue = getRangeUpperBound(subPartition);
            ddl.append(" VALUES LESS THAN (").append(highValue).append(")");
        } else if (subPartitionKey.startsWith("LIST")) {
            String value = getListPartitionValue(subPartition);
            ddl.append(" VALUES (").append(value).append(")");
        }
        // Hash subpartition doesn't need values clause
    }

    /**
     * Extract subpartition key from partition information
     *
     * @param schema    Schema name
     * @param tableName Table name
     * @param conn      Database connection
     * @return Subpartition key
     * @throws SQLException SQL exception
     */
    String extractSubPartitionKey(String schema, String tableName, Connection conn) throws SQLException;

    /**
     * Get range upper bound from partition info
     *
     * @param partition Partition information
     * @return Range upper bound
     */
    default String getRangeUpperBound(PartitionInfo partition) {
        String highValue = partition.getRangeUpperBound();
        return highValue != null && !highValue.isEmpty() ? highValue : "MAXVALUE";
    }

    /**
     * Get list partition value from partition info
     *
     * @param partition Partition information
     * @return List partition value
     */
    default String getListPartitionValue(PartitionInfo partition) {
        return partition.getListPartitionValue();
    }

    /**
     * Partition DDL context class
     */
    @Data
    class PartitionDdlContext {
        private String schema;
        private String tableName;
        private Connection conn;
        private String partitionKey;
        private List<PartitionInfo> partitions;
        private Map<String, List<PartitionInfo>> subPartitionsMap;
    }

    /**
     * Partition metadata class
     */
    @Data
    class PartitionMetadata {
        private String partitionKey;
        private String subPartitionKey;
        private boolean isPartitioned;
        private boolean isSubPartitioned;
        private String intervalDefinition;
        private String partitioningType;
        private String subPartitioningType;
    }
}
