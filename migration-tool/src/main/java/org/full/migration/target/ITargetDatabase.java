/*
 * Copyright 2026 Huawei Technologies Co., Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.full.migration.target;

import org.full.migration.exception.DatabaseConnectionException;
import org.full.migration.exception.MigrationException;
import org.full.migration.model.PostgresCustomTypeMeta;
import org.full.migration.model.table.TableData;

import java.sql.Connection;
import java.util.List;
import java.util.Set;

/**
 * ITargetDatabase
 * Interface for target database operations during migration
 * Defines methods for creating schema objects, writing data, and managing constraints
 * 
 * @since 2025-04-18
 */
public interface ITargetDatabase {
    /**
     * Check the connection to the target database  
     * 
     * @throws DatabaseConnectionException
     */
    void checkConnection() throws DatabaseConnectionException;
    /**
     * Insert table snapshot information into the target database
     * 
     * @param conn      Database connection
     * @param tableData Table data information
     */
    void insertTableSnapshotInfo(Connection conn, TableData tableData);
    
    /**
     * Create schemas in the target database
     * 
     * @param schemas Set of schema names to create
     * @throws MigrationException 
     */
     void createSchemas(Set<String> schemas) throws MigrationException;
    
    /**
     * Write constraints to the target database
     */
    void writeConstraints();
    
    /**
     * Write table indexes to the target database
     */
    void writeTableIndex();
    
    /**
     * Write table primary keys to the target database
     */
    void writeTablePk();
    
    /**
     * Write table foreign keys to the target database
     */
    void writeTableFk();
    
    /**
     * Write table structure to the target database
     */
    void writeTableConstruct();
    
    /**
     * Write table data to the target database
     */
    void writeTable();
    
    /**
     * Write database objects to the target database
     * 
     * @param sourceDbType Source database type
     * @param objectType   Type of objects to write
     */
    void writeObjects(String sourceDbType, String objectType);
    
    /**
     * Drop replica schema in the target database
     */
    void dropReplicaSchema();
    
    /**
     * Create custom or domain types in the target database
     * 
     * @param customTypes List of custom type metadata
     */
    void createCustomOrDomainTypes(List<PostgresCustomTypeMeta> customTypes);
    
    /**
     * Shutdown the target database instance and release resources
     */
    void shutdown();
}
