# SQL Server 迁移功能使用说明书

## 1 迁移限制

### 1.1 版本限制

- **源端**：SQL Server 2016 及以上版本
- **目标端**：openGauss 7.0.0 及以上版本

### 1.2 兼容性要求

- 目标数据库必须为 **D 兼容模式**（`dbcompatibility='D'`）
- 目标数据库需要开启 **shark Extension** 扩展，以兼容 SQL Server 系统函数等语法特性。该扩展是 openGauss D 兼容数据库的语法增强模块。

### 1.3 功能限制

#### 1.3.1 不支持迁移的对象

- 表和列的注释
- 几何类型（`geometry`、`geography`）
- 部分索引类型：列存储索引、全文索引、XML 索引、包含列索引、索引填充因子

#### 1.3.2 数据类型精度差异

- `real`（SQL Server，约7位精度）→ openGauss（6位精度）
- `float`（SQL Server，15~16位）→ openGauss（15位）
- `time`、`datetime2`、`datetimeoffset`：SQL Server 默认精度为7，openGauss 仅支持0~6，可通过 `isTimeMigrate` 配置决定是否迁移（接受精度损失）
- `money` / `smallmoney`：SQL Server 精度为货币单位的万分之一，openGauss 为百分之一，可通过 `isMoneyMigrate` 配置控制是否迁移

#### 1.3.3 分区表限制

- SQL Server 分区表底层均基于 Range 分区实现，迁移至 openGauss 后统一为 Range 分区类型。

#### 1.3.4 对象迁移限制

- 支持迁移：视图、函数、触发器、存储过程
- 由于 D 库兼容性仍在持续完善中，部分 SQL Server 特有语法可能迁移失败
- 当前仅保证对象迁移流程可执行，实际迁移成功与否取决于语法兼容性

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
  host: "192.168.0.2"
  port: 5432
  user: "username"
  password: "password"
  database: "database"
 
# 源端 SQL Server 配置
sourceConfig:
  # 读取表的并发线程数
  readerNum: 4
  # 写入表的并发线程数
  writerNum: 4
  # CSV 数据文件拆分大小，支持 K/M/G 单位，默认 M
  fileSize: 2M
  
  # 源端数据库连接信息
  dbConn:
    host: "192.168.0.1"
    port: "1433"
    user: "username"
    password: "password"
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
  
  # 是否迁移 time / datetime2 / datetimeoffset 类型（可能损失精度）
  isTimeMigrate: true

  # 是否迁移 money / smallmoney 类型（可能损失精度）
  isMoneyMigrate: true
```

## 3 迁移命令

### 3.1 参考命令

完成配置文件修改后，根据所需迁移的对象类型执行对应命令：

```bash
# 迁移表
java -jar openGauss-FullReplicate-7.0.0-RC3.jar --start table --source sqlserver --config /path/to/config.yml

# 迁移主键
java -jar openGauss-FullReplicate-7.0.0-RC3.jar --start primarykey --source sqlserver --config /path/to/config.yml

# 迁移外键
java -jar openGauss-FullReplicate-7.0.0-RC3.jar --start foreignkey --source sqlserver --config /path/to/config.yml

# 迁移索引
java -jar openGauss-FullReplicate-7.0.0-RC3.jar --start index --source sqlserver --config /path/to/config.yml

# 迁移约束
java -jar openGauss-FullReplicate-7.0.0-RC3.jar --start constraint --source sqlserver --config /path/to/config.yml

# 迁移视图
java -jar openGauss-FullReplicate-7.0.0-RC3.jar --start view --source sqlserver --config /path/to/config.yml

# 迁移函数
java -jar openGauss-FullReplicate-7.0.0-RC3.jar --start function --source sqlserver --config /path/to/config.yml

# 迁移触发器
java -jar openGauss-FullReplicate-7.0.0-RC3.jar --start trigger --source sqlserver --config /path/to/config.yml

# 迁移存储过程
java -jar openGauss-FullReplicate-7.0.0-RC3.jar --start procedure --source sqlserver --config /path/to/config.yml

# 迁移序列
java -jar openGauss-FullReplicate-7.0.0-RC3.jar --start sequence --source sqlserver --config /path/to/config.yml
```

### 3.2 命令参数说明

| 参数       | 说明                                                 |
| :--------- | :--------------------------------------------------- |
| `--start`  | 指定迁移的对象类型（如 `table`、`index`、`view` 等） |
| `--source` | 指定源数据库类型，SQL Server 迁移请使用 `sqlserver`  |
| `--config` | 指定配置文件路径                                     |

## 4 迁移规则

### 4.1 数据类型映射

| SQL Server       | openGauss                      | 备注                                                         |
| :--------------- | :----------------------------- | :----------------------------------------------------------- |
| tinyint          | tinyint                        | 兼容                                                         |
| smallint         | smallint                       | 兼容                                                         |
| int              | int                            | 兼容                                                         |
| bigint           | bigint                         | 兼容                                                         |
| bit              | boolean                        | 兼容                                                         |
| decimal          | decimal                        | 兼容                                                         |
| numeric          | numeric                        | 兼容                                                         |
| money            | money                          | 兼容                                                         |
| smallmoney       | money                          | 兼容                                                         |
| float            | float                          | 兼容，float(24)会以real类型迁移                              |
| real             | real                           | 兼容，相当于float(24)                                        |
| double precision | float                          | 兼容，相当于float(53)                                        |
| char             | char                           | 兼容                                                         |
| varchar          | varchar                        | 兼容                                                         |
| nchar            | nchar                          | 兼容                                                         |
| nvarchar         | nvarchar                       | 兼容                                                         |
| ntext            | text                           | 兼容                                                         |
| binary           | bytea                          | 兼容，bytea不指定长度，最大为1GB-8203字节                    |
| varbinary        | bytea                          | 兼容，bytea不指定长度，最大为1GB-8203字节                    |
| image            | text                           | 暂未支持longtext类型，后续兼容性适配，超范围的文本会报错     |
| text             | text                           | 暂未支持longtext类型，后续兼容性适配，超范围的文本会报错     |
| date             | date                           | 兼容                                                         |
| datetime         | timestamp                      | 兼容                                                         |
| smalldatetime    | timestamp                      | 兼容                                                         |
| time             | time[(p)][without time zone]   | SQL Server精度0-7，默认7，openGauss仅支持0-6，通过配置文件的isTimeMigrate参数控制是否接受精度损失 |
| datetime2        | timestamp                      | SQL Server精度0-7，默认7，openGauss仅支持0-6，通过配置文件的isTimeMigrate参数控制是否接受精度损失 |
| datetimeoffset   | timestamp[(p)][with time zone] | SQL Server精度0-7，默认7，openGauss仅支持0-6，通过配置文件的isTimeMigrate参数控制是否接受精度损失 |
| hierarchyid      | nvarchar(4000)                 | 不兼容                                                       |
| json             | jsonb                          | SQLServer的json类型底层为nvarchar                            |
| rowversion       | timestamp                      | jdbc查询该类型为timestamp                                    |
| sql_variant      |                                | 暂不支持，后续兼容性适配                                     |
| uniqueidentifier | uuid                           | 兼容                                                         |
| xml              | xml                            | 兼容                                                         |

### 4.2 索引类型映射

| SQL Server          | openGauss       | 备注                   |
| :------------------ | :-------------- | :--------------------- |
| Clustered Index     | B-tree Index    | 待兼容性支持           |
| Non-Clustered Index | B-tree Index    | 待兼容性支持           |
| Unique Index        | Unique Index    | 待兼容性支持           |
| Full-Text Index     | -               | 不兼容                 |
| Spatial Index       | Gist Index      | 兼容                   |
| XML Index           | -               | 不兼容                 |
| Filtered Index      | 索引including列 | 仅Ustore类型数据库支持 |