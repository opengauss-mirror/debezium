# PostgreSQL 迁移功能使用说明书

## 1 迁移限制

### 1.1 版本限制

- **源端**：PostgreSQL 9.4.26 及以上版本
- **目标端**：openGauss 6.0.0 及以上版本

### 1.2 兼容性要求

- 目标数据库必须为 **PG 兼容模式**（`dbcompatibility='PG'`）

## 2 配置说明

配置文件模板位于 `config/` 目录下，请根据实际迁移环境配置以下参数：

```yaml
# 是否记录迁移进度
isDumpJson: true
# 进度文件存储路径
statusDir: /**/**

# 是否删除迁移成功后的 CSV 中间文件
isDeleteCsv: true

# 是否保留目标端原有 schema
isKeepExistingSchema: false

# 目标端 openGauss 数据库配置
ogConn:
  host: "127.0.0.1"
  port: 5432
  user: "username"
  password: "******"
  database: "database"

# 源端 PostgreSQL 配置
sourceConfig:
  # 读取表的并发线程数
  readerNum: 4
  # 写入表的并发线程数
  writerNum: 4
  # CSV 数据文件拆分大小，支持 K/M/G 单位，默认 M
  fileSize: 2M

  # 源端数据库连接信息
  dbConn:
    host: "127.0.0.1"
    port: 5432
    user: "username"
    password: "******"
    database: "database"

  # Schema 名称映射（源端: 目标端）
  schemaMappings:
  #  schema1: ogschema1
  #  schema2: ogschema2
  
  # 表白名单（仅迁移指定表，不配置则迁移所有表）
  limitTables:
  # - schema1.table1
  # - schema1.table2

  # 表黑名单（排除指定表）
  skipTables:
  # - schema1.table4

  # CSV 文件存储路径
  csvDir: /**/**

  # 是否记录快照（配合增量迁移使用）
  isRecordSnapshot: false
  # 增量迁移使用的 PostgreSQL 逻辑复制槽名称
  slotName: "test_slot"
  # 逻辑复制插件名称（PostgreSQL ≥10 推荐 pgoutput，备选 wal2json）
  pluginName: "pgoutput"
```

## 3 迁移命令

### 3.1 参考命令

完成配置文件修改后，根据所需迁移的对象类型执行对应命令：

```bash
# 迁移表
java -jar openGauss-FullReplicate-7.0.0-RC3.jar --start table --source postgresql --config /path/to/config.yml

# 迁移主键
java -jar openGauss-FullReplicate-7.0.0-RC3.jar --start primarykey --source postgresql --config /path/to/config.yml

# 迁移外键
java -jar openGauss-FullReplicate-7.0.0-RC3.jar --start foreignkey --source postgresql --config /path/to/config.yml

# 迁移索引
java -jar openGauss-FullReplicate-7.0.0-RC3.jar --start index --source postgresql --config /path/to/config.yml

# 迁移约束
java -jar openGauss-FullReplicate-7.0.0-RC3.jar --start constraint --source postgresql --config /path/to/config.yml

# 迁移视图
java -jar openGauss-FullReplicate-7.0.0-RC3.jar --start view --source postgresql --config /path/to/config.yml

# 迁移函数
java -jar openGauss-FullReplicate-7.0.0-RC3.jar --start function --source postgresql --config /path/to/config.yml

# 迁移触发器
java -jar openGauss-FullReplicate-7.0.0-RC3.jar --start trigger --source postgresql --config /path/to/config.yml

# 迁移存储过程
java -jar openGauss-FullReplicate-7.0.0-RC3.jar --start procedure --source postgresql --config /path/to/config.yml

# 迁移序列
java -jar openGauss-FullReplicate-7.0.0-RC3.jar --start sequence --source postgresql --config /path/to/config.yml
```

### 3.2 命令参数说明

| 参数       | 说明                                                 |
| :--------- | :--------------------------------------------------- |
| `--start`  | 指定迁移的对象类型（如 `table`、`index`、`view` 等） |
| `--source` | 指定源数据库类型，PostgreSQL 迁移请使用 `postgresql` |
| `--config` | 指定配置文件路径                                     |

## 4 迁移规则

### 4.1 数据类型映射

| PostgreSQL                              | openGauss                               | 备注                            |
| :-------------------------------------- | :-------------------------------------- | :------------------------------ |
| smallint                                | smallint                                |                                 |
| integer                                 | integer                                 |                                 |
| bigint                                  | bigint                                  |                                 |
| decimal                                 | decimal                                 |                                 |
| numeric                                 | numeric                                 |                                 |
| real                                    | real                                    |                                 |
| double precision                        | double precision                        |                                 |
| smallserial                             | smallserial                             |                                 |
| serial                                  | serial                                  |                                 |
| bigserial                               | bigserial                               |                                 |
| money                                   | money                                   |                                 |
| varchar(n) character varying(n)         | varchar(n) character varying(n)         |                                 |
| char(n), character(n)                   | char(n), character(n)                   |                                 |
| varchar                                 | varchar                                 |                                 |
| char                                    | char                                    |                                 |
| text                                    | text                                    |                                 |
| name                                    | name                                    |                                 |
| bytea                                   | bytea                                   |                                 |
| timestamp [ (p) ] [ without time zone ] | timestamp [ (p) ] [ without time zone ] |                                 |
| timestamp [ (p) ] with time zone        | timestamp [ (p) ] with time zone        |                                 |
| timestamp without time zone             | timestamp(6) without time zone          | pg侧创建时不带精度，默认精度为6 |
| timestamp with time zone                | timestamp(6) with time zone             | pg侧创建时不带精度，默认精度为6 |
| date                                    | date                                    |                                 |
| time [ (p) ] [ without time zone ]      | time [ (p) ] [ without time zone ]      |                                 |
| time [ (p) ] with time zone             | time [ (p) ] with time zone             |                                 |
| time without time zone                  | time(6) without time zone               | pg侧创建时不带精度，默认精度为6 |
| time with time zone                     | time(6) with time zone                  | pg侧创建时不带精度，默认精度为6 |
| interval [ fields ] [ (p) ]             | interval [ fields ] [ (p) ]             |                                 |
| interval                                | interval(6)                             | pg侧创建时不带精度，默认精度为6 |
| boolean                                 | boolean                                 |                                 |
| oid                                     | oid                                     |                                 |
| enum                                    | enum                                    |                                 |
| point                                   | point                                   |                                 |
| line                                    | varchar                                 | og不支持，转换成varchar类型     |
| lseg                                    | lseg                                    |                                 |
| box                                     | box                                     |                                 |
| path                                    | path                                    |                                 |
| polygon                                 | polygon                                 |                                 |
| circle                                  | circle                                  |                                 |
| cidr                                    | cidr                                    |                                 |
| inet                                    | inet                                    |                                 |
| macaddr                                 | macaddr                                 |                                 |
| bit                                     | bit(1)                                  | pg侧bit默认精度为1              |
| bit(n)                                  | bit(n)                                  |                                 |
| bit varying(n)                          | bit varying(n)                          |                                 |
| tsvector                                | tsvector                                |                                 |
| tsquery                                 | tsquery                                 |                                 |
| uuid                                    | uuid                                    |                                 |
| xml                                     | xml                                     |                                 |
| json                                    | json                                    |                                 |
| jsonb                                   | jsonb                                   |                                 |
| array                                   | array                                   |                                 |
| Composite Types（组合类型）             | Composite Types                         |                                 |
| int4range                               | int4range                               |                                 |
| int8range                               | int8range                               |                                 |
| numrange                                | numrange                                |                                 |
| tsrange                                 | tsrange                                 |                                 |
| tstzrange                               | tstzrange                               |                                 |
| daterange                               | daterange                               |                                 |
| 域类型                                  | 域类型                                  |                                 |
| pg_lsn                                  | varchar                                 | og不支持，使用varchar替代       |
