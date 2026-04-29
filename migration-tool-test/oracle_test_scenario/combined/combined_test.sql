-- Combined test script to execute all other test scenarios
-- This script runs all test scripts from other directories except the combined directory itself

-- Execute scripts from comment directory

-- @table_structure/oracle_normal_tables_test.sql
-- @table_structure/oracle_partition_tables_test.sql
-- @table_structure/table_structure_test.sql
 @noprimary/noprimary_test.sql
-- @zhongwen/test_zhongwen_data.sql
-- @comment/comment_test.sql
-- @data_type/data_type_test.sql
-- @data_type/oracle_all_types_test.sql
-- @data_type/oracle_float_types_test.sql

-- @sequence/sequence_test.sql
-- @constraint/constraint_test.sql
-- @foreign_key/foreign_key_test.sql

-- @index/index_test.sql
-- @index/oracle_index_types_test.sql
-- @index/oracle_all_index_types_test.sql

-- @function/function_test.sql
-- @procedure/procedure_test.sql
-- @trigger/trigger_test.sql
-- @view/view_test.sql
