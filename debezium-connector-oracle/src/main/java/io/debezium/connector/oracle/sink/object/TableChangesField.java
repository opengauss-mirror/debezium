/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.sink.object;

import java.util.*;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.Struct;

/**
 * Description: TableChangesField class
 *
 * @author gbase
 * @date 2023/08/07
 **/
public class TableChangesField {
    /**
     * TABLE_CHANGES
     */
    public static final String TABLE_CHANGES = "tableChanges";
    /**
     * TYPE
     */
    public static final String TYPE = "type";
    /**
     * TABLE
     */
    public static final String TABLE = "table";
    /**
     * DEFAULT_CHARSET_NAME
     */
    public static final String DEFAULT_CHARSET_NAME = "defaultCharsetName";
    /**
     * PRIMARY_KEY_COLUMN_NAMES
     */
    public static final String PRIMARY_KEY_COLUMN_NAMES = "primaryKeyColumnNames";
    /**
     * PRIMARY_KEY_COLUMN_CHANGES
     */
    public static final String PRIMARY_KEY_COLUMN_CHANGES = "primaryKeyColumnChanges";
    /**
     * FOREIGN_KEY_COLUMNS
     */
    public static final String FOREIGN_KEY_COLUMNS = "foreignKeyColumns";
    /**
     * UNIQUE_COLUMNS
     */
    public static final String UNIQUE_COLUMNS = "uniqueColumns";
    /**
     * CHECK_COLUMNS
     */
    public static final String CHECK_COLUMNS = "checkColumns";
    /**
     * COLUMNS
     */
    public static final String COLUMNS = "columns";
    /**
     * CHANGE_COLUMNS
     */
    public static final String CHANGE_COLUMNS = "changeColumns";

    private final String type;
    private final String defaultCharsetName;
    private final List<ColumnField> columnFields;
    private final LinkedHashMap<String, ColumnField> columnFieldsMap = new LinkedHashMap<>();
    private final List<String> primaryKeyColumnNames;
    private List<PrimaryKeyColumnChangesField> primaryKeyColumnChanges = new ArrayList<>();
    private final List<ForeignKeyColumnsField> foreignKeyColumns;
    private final List<UniqueColumnsField> uniqueColumns;
    private final List<CheckColumnsField> checkColumns;
    private ChangeColumnsField changeColumns;

    /**
     * Constructor
     *
     * @param value source value
     */
    public TableChangesField(Struct value) {
        if (value == null) {
            throw new IllegalArgumentException("value can't be null!");
        }
        List<Object> tableChangesArr = value.getArray(TableChangesField.TABLE_CHANGES);
        if (tableChangesArr == null || tableChangesArr.isEmpty()) {
            throw new IllegalArgumentException("tableChanges can't be null or empty!");
        }
        Object tableChangedObj = tableChangesArr.get(0);
        if (!(tableChangedObj instanceof Struct)) {
            throw new IllegalArgumentException("tableChanges is not a Struct.");
        }
        Struct tableChanges = (Struct) tableChangedObj;
        this.type = tableChanges.getString(TableChangesField.TYPE);
        Struct table = tableChanges.getStruct(TableChangesField.TABLE);
        if (table == null) {
            throw new IllegalArgumentException("table can't be null!");
        }
        // defaultCharsetName
        this.defaultCharsetName = table.getString(TableChangesField.DEFAULT_CHARSET_NAME);
        // columns
        List<Object> columns = table.getArray(TableChangesField.COLUMNS);
        this.columnFields = new ArrayList<>();
        for (Object columnObj : columns) {
            if (columnObj instanceof Struct) {
                ColumnField columnField = new ColumnField((Struct) columnObj);
                columnFields.add(columnField);
                this.columnFieldsMap.put(columnField.getName(), columnField);
            }
        }
        // primaryKeyColumnNames
        List<Object> pkColumns = table.getArray(TableChangesField.PRIMARY_KEY_COLUMN_NAMES);
        this.primaryKeyColumnNames = pkColumns.stream().map(Object::toString).collect(Collectors.toList());

        // primaryKeyColumnChanges
        List<Object> pkColumnChanges = table.getArray(TableChangesField.PRIMARY_KEY_COLUMN_CHANGES);
        if (pkColumnChanges != null) {
            this.primaryKeyColumnChanges = pkColumnChanges.stream()
                    .map(o -> o instanceof Map ? new PrimaryKeyColumnChangesField(((Map) o)) : null)
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());
        }

        // foreignKeyColumns
        List<Object> fkColumns = table.getArray(TableChangesField.FOREIGN_KEY_COLUMNS);
        this.foreignKeyColumns = fkColumns.stream()
                .map(o -> o instanceof Map ? new ForeignKeyColumnsField(((Map) o)) : null)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());

        // uniqueColumns
        List<Object> uniqueColumnList = table.getArray(TableChangesField.UNIQUE_COLUMNS);
        this.uniqueColumns = uniqueColumnList.stream()
                .map(o -> o instanceof Map ? new UniqueColumnsField(((Map) o)) : null)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());

        // check
        List<Object> checkColumnList = table.getArray(TableChangesField.CHECK_COLUMNS);
        this.checkColumns = checkColumnList.stream()
                .map(o -> o instanceof Map ? new CheckColumnsField(((Map) o)) : null)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());

        // changeColumns
        Map<Object, Object> changeColumnMap = table.getMap(TableChangesField.CHANGE_COLUMNS);
        if (changeColumnMap != null) {
            this.changeColumns = new ChangeColumnsField(changeColumnMap);
        }
    }

    public String getType() {
        return type;
    }

    public String getDefaultCharsetName() {
        return defaultCharsetName;
    }

    public List<ColumnField> getColumnFields() {
        return columnFields;
    }

    public List<String> getPrimaryKeyColumnNames() {
        return primaryKeyColumnNames;
    }

    public List<PrimaryKeyColumnChangesField> getPrimaryKeyColumnChanges() {
        return primaryKeyColumnChanges;
    }

    public List<ForeignKeyColumnsField> getForeignKeyColumns() {
        return foreignKeyColumns;
    }

    public List<UniqueColumnsField> getUniqueColumns() {
        return uniqueColumns;
    }

    public List<CheckColumnsField> getCheckColumns() {
        return checkColumns;
    }

    public ChangeColumnsField getChangeColumns() {
        return changeColumns;
    }

    public LinkedHashMap<String, ColumnField> getColumnFieldsMap() {
        return columnFieldsMap;
    }

    @Override
    public String toString() {
        return "TableChangesField{" +
                "type='" + type + '\'' +
                ", defaultCharsetName='" + defaultCharsetName + '\'' +
                ", columnFields=" + columnFields +
                ", columnFieldsMap=" + columnFieldsMap +
                ", primaryKeyColumnNames=" + primaryKeyColumnNames +
                ", foreignKeyColumns=" + foreignKeyColumns +
                ", uniqueColumns=" + uniqueColumns +
                ", checkColumns=" + checkColumns +
                ", changeColumns=" + changeColumns +
                '}';
    }

    /**
     * Column Field
     */
    public static class ColumnField {
        /**
         * COLUMNS
         */
        public static final String COLUMNS = "columns";
        /**
         * NAME
         */
        public static final String NAME = "name";
        /**
         * JDBC_TYPE
         */
        public static final String JDBC_TYPE = "jdbcType";
        /**
         * TYPE_NAME
         */
        public static final String TYPE_NAME = "typeName";
        /**
         * TYPE_EXPRESSION
         */
        public static final String TYPE_EXPRESSION = "typeExpression";
        /**
         * CHARSET_NAME
         */
        public static final String CHARSET_NAME = "charsetName";
        /**
         * LENGTH
         */
        public static final String LENGTH = "length";
        /**
         * SCALE
         */
        public static final String SCALE = "scale";
        /**
         * POSITION
         */
        public static final String POSITION = "position";
        /**
         * OPTIONAL
         */
        public static final String OPTIONAL = "optional";
        /**
         * DEFAULT_VALUE_EXPRESSION
         */
        public static final String DEFAULT_VALUE_EXPRESSION = "defaultValueExpression";
        /**
         * AUTO_INCREMENTED
         */
        public static final String AUTO_INCREMENTED = "autoIncremented";
        /**
         * GENERATED
         */
        public static final String GENERATED = "generated";
        /**
         * COMMENT
         */
        public static final String COMMENT = "comment";
        /**
         * MODIFY_KEYS
         */
        public static final String MODIFY_KEYS = "modifyKeys";

        private final String name;
        private final int jdbcType;
        private final String typeName;
        private final String typeExpression;
        private final String charsetName;
        private final Integer length;
        private final Integer scale;
        private final int position;
        private final boolean optional;
        private final String defaultValueExpression;
        private final boolean autoIncremented;
        private final boolean generated;
        private final String comment;
        private List<String> modifyKeys;

        /**
         * Constructor
         *
         * @param value column value
         */
        public ColumnField(Struct value) {
            if (value == null) {
                throw new IllegalArgumentException("value can't be null!");
            }
            this.name = value.getString(NAME);
            this.jdbcType = value.getInt32(JDBC_TYPE);
            this.typeName = value.getString(TYPE_NAME);
            this.typeExpression = value.getString(TYPE_EXPRESSION);
            this.charsetName = value.getString(CHARSET_NAME);
            this.length = value.getInt32(LENGTH);
            this.scale = value.getInt32(SCALE);
            this.position = value.getInt32(POSITION);
            this.optional = value.getBoolean(OPTIONAL);
            this.defaultValueExpression = value.getString(DEFAULT_VALUE_EXPRESSION);
            this.autoIncremented = value.getBoolean(AUTO_INCREMENTED);
            this.generated = value.getBoolean(GENERATED);
            this.comment = value.getString(COMMENT);
            List<Object> modifyKeysArr = value.getArray(MODIFY_KEYS);
            if (modifyKeysArr != null) {
                this.modifyKeys = modifyKeysArr.stream()
                        .map(Object::toString)
                        .collect(Collectors.toList());
            }
        }

        public String getName() {
            return name;
        }

        public int getJdbcType() {
            return jdbcType;
        }

        public String getTypeName() {
            return typeName;
        }

        public String getTypeExpression() {
            return typeExpression;
        }

        public String getCharsetName() {
            return charsetName;
        }

        public Integer getLength() {
            return length;
        }

        public Integer getScale() {
            return scale;
        }

        public int getPosition() {
            return position;
        }

        public boolean isOptional() {
            return optional;
        }

        public String getDefaultValueExpression() {
            return defaultValueExpression;
        }

        public boolean isAutoIncremented() {
            return autoIncremented;
        }

        public boolean isGenerated() {
            return generated;
        }

        public String getComment() {
            return comment;
        }

        public List<String> getModifyKeys() {
            return modifyKeys;
        }

        @Override
        public String toString() {
            return "ColumnField{" +
                    "name='" + name + '\'' +
                    ", jdbcType=" + jdbcType +
                    ", typeName='" + typeName + '\'' +
                    ", typeExpression='" + typeExpression + '\'' +
                    ", charsetName='" + charsetName + '\'' +
                    ", length=" + length +
                    ", scale=" + scale +
                    ", position=" + position +
                    ", optional=" + optional +
                    ", defaultValueExpression='" + defaultValueExpression + '\'' +
                    ", autoIncremented=" + autoIncremented +
                    ", generated=" + generated +
                    ", comment='" + comment + '\'' +
                    ", modifyKeys=" + modifyKeys +
                    '}';
        }
    }

    /**
     * PrimaryKeyColumnChangesField
     */
    public static class PrimaryKeyColumnChangesField {
        /**
         * VALUE_ACTION_ADD
         */
        public static final String VALUE_ACTION_ADD = "add";
        /**
         * VALUE_ACTION_DROP
         */
        public static final String VALUE_ACTION_DROP = "drop";
        /**
         * ACTION
         */
        public static final String ACTION = "action";
        /**
         * CONSTRAINT_NAME
         */
        public static final String CONSTRAINT_NAME = "constraintName";
        /**
         * COLUMN_NAME
         */
        public static final String COLUMN_NAME = "columnName";
        /**
         * CASCADE
         */
        public static final String CASCADE = "cascade";
        /**
         * TYPE
         */
        public static final String TYPE = "type";

        private final String action;
        private final String constraintName;
        private final String columnName;
        private final String cascade;
        private final String type;

        /**
         * Constructor
         *
         * @param value changes value
         */
        public PrimaryKeyColumnChangesField(Map<String, String> value) {
            if (value == null) {
                throw new IllegalArgumentException("value can't be null!");
            }

            this.action = value.get(ACTION);
            this.constraintName = value.get(CONSTRAINT_NAME);
            this.columnName = value.get(COLUMN_NAME);
            this.cascade = value.get(CASCADE);
            this.type = value.get(TYPE);
        }

        public String getAction() {
            return action;
        }

        public String getConstraintName() {
            return constraintName;
        }

        public String getColumnName() {
            return columnName;
        }

        public String getCascade() {
            return cascade;
        }

        public String getType() {
            return type;
        }

        @Override
        public String toString() {
            return "PrimaryKeyColumnChangesField{" +
                    "action='" + action + '\'' +
                    ", constraintName='" + constraintName + '\'' +
                    ", columnName='" + columnName + '\'' +
                    ", cascade='" + cascade + '\'' +
                    ", type='" + type + '\'' +
                    '}';
        }
    }

    /**
     * ForeignKeyColumnsField
     */
    public static class ForeignKeyColumnsField {
        /**
         * PK_COLUMN_NAME
         */
        public static final String PK_COLUMN_NAME = "pkColumnName";
        /**
         * PK_TABLE_NAME
         */
        public static final String PK_TABLE_NAME = "pktableName";
        /**
         * PK_TABLE_SCHEMA
         */
        public static final String PK_TABLE_SCHEMA = "pktableSchem";
        /**
         * FK_NAME
         */
        public static final String FK_NAME = "fkName";
        /**
         * FK_COLUMN_NAME
         */
        public static final String FK_COLUMN_NAME = "fkColumnName";
        /**
         * CASCADE
         */
        public static final String CASCADE = "cascade";

        private final String pkColumnName;
        private final String pkTableName;
        private final String pkTableSchema;
        private final String fkName;
        private final String fkColumnName;
        private final String cascade;

        /**
         * Constructor
         *
         * @param value foreign key value
         */
        public ForeignKeyColumnsField(Map<String, String> value) {
            if (value == null) {
                throw new IllegalArgumentException("value can't be null!");
            }

            this.pkColumnName = value.get(PK_COLUMN_NAME);
            this.pkTableName = value.get(PK_TABLE_NAME);
            this.pkTableSchema = value.get(PK_TABLE_SCHEMA);
            this.fkName = value.get(FK_NAME);
            this.fkColumnName = value.get(FK_COLUMN_NAME);
            this.cascade = value.get(CASCADE);
        }

        public String getPkColumnName() {
            return pkColumnName;
        }

        public String getPkTableName() {
            return pkTableName;
        }

        public String getPkTableSchema() {
            return pkTableSchema;
        }

        public String getFkName() {
            return fkName;
        }

        public String getFkColumnName() {
            return fkColumnName;
        }

        public String getCascade() {
            return cascade;
        }

        @Override
        public String toString() {
            return "ForeignKeyColumnsField{" +
                    "pkColumnName='" + pkColumnName + '\'' +
                    ", pkTableName='" + pkTableName + '\'' +
                    ", pkTableSchema='" + pkTableSchema + '\'' +
                    ", fkName='" + fkName + '\'' +
                    ", fkColumnName='" + fkColumnName + '\'' +
                    ", cascade='" + cascade + '\'' +
                    '}';
        }
    }

    /**
     * UniqueColumnsField
     */
    public static class UniqueColumnsField {
        /**
         * INDEX_NAME
         */
        public static final String INDEX_NAME = "indexName";
        /**
         * COLUMN_NAME
         */
        public static final String COLUMN_NAME = "columnName";

        private final String indexName;
        private final String columnName;

        /**
         * Constructor
         *
         * @param value unique value
         */
        public UniqueColumnsField(Map<String, String> value) {
            if (value == null) {
                throw new IllegalArgumentException("value can't be null!");
            }

            this.indexName = value.get(INDEX_NAME);
            this.columnName = value.get(COLUMN_NAME);
        }

        public String getIndexName() {
            return indexName;
        }

        public String getColumnName() {
            return columnName;
        }

        @Override
        public String toString() {
            return "UniqueColumnsField{" +
                    "indexName='" + indexName + '\'' +
                    ", columnName='" + columnName + '\'' +
                    '}';
        }
    }

    /**
     * CheckColumnsField
     */
    public static class CheckColumnsField {
        /**
         * INDEX_NAME
         */
        public static final String INDEX_NAME = "indexName";
        /**
         * CONDITION
         */
        public static final String CONDITION = "condition";
        /**
         * INCLUDE_COLUMN
         */
        public static final String INCLUDE_COLUMN = "includeColumn";

        private final String indexName;
        private final String condition;
        private final String includeColumn;

        /**
         * Constructor
         *
         * @param value check value
         */
        public CheckColumnsField(Map<String, String> value) {
            if (value == null) {
                throw new IllegalArgumentException("value can't be null!");
            }

            this.indexName = value.get(INDEX_NAME);
            this.condition = value.get(CONDITION);
            this.includeColumn = value.get(INCLUDE_COLUMN);
        }

        public String getIndexName() {
            return indexName;
        }

        public String getCondition() {
            return condition;
        }

        public String getIncludeColumn() {
            return includeColumn;
        }

        @Override
        public String toString() {
            return "CheckColumnsField{" +
                    "indexName='" + indexName + '\'' +
                    ", condition='" + condition + '\'' +
                    ", includeColumn='" + includeColumn + '\'' +
                    '}';
        }
    }

    /**
     * ChangeColumnsField
     */
    public static class ChangeColumnsField {
        /**
         * ADD_COLUMNS
         */
        public static final String ADD_COLUMNS = "addColumn";
        /**
         * MODIFY_COLUMNS
         */
        public static final String MODIFY_COLUMNS = "modifyColumn";
        /**
         * DROP_COLUMNS
         */
        public static final String DROP_COLUMNS = "dropColumn";

        private final String changeType;
        private final List<String> columnNames;

        /**
         * Constructor
         *
         * @param changeColumnMap changeColumns map
         */
        public ChangeColumnsField(Map<Object, Object> changeColumnMap) {
            if (changeColumnMap == null) {
                throw new IllegalArgumentException("changeColumns can't be null!");
            }

            if (changeColumnMap.isEmpty()) {
                throw new IllegalArgumentException("changeColumns can't be empty!");
            }

            Iterator<Map.Entry<Object, Object>> iterator = changeColumnMap.entrySet().iterator();
            Map.Entry<Object, Object> entry = iterator.next();

            this.changeType = entry.getKey().toString();
            this.columnNames = ((Collection<?>) entry.getValue()).stream()
                    .map(Object::toString)
                    .collect(Collectors.toList());
        }

        public String getChangeType() {
            return changeType;
        }

        public List<String> getColumnNames() {
            return columnNames;
        }

        @Override
        public String toString() {
            return "ChangeColumnsField{" +
                    "changeType='" + changeType + '\'' +
                    ", columnNames=" + columnNames +
                    '}';
        }
    }
}
