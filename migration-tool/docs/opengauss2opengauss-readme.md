# openGauss 迁移功能使用说明书

## 1 迁移限制

### 1.1 版本限制

- **源端**：openGauss 5.0.0 及以上版本
- **目标端**：目标端 openGauss 版本需要大于等于源端版本

### 1.2 兼容性要求

- 当前仅支持迁移 **A 兼容模式** 和 **B 兼容模式** 的数据库
- 目标数据库的兼容模式必须与源端保持一致

### 1.3 功能限制

- **外表**：暂不支持迁移
- **资源池化参数**：openGauss 资源池化模式下 `enable_segment` 默认值为 `on`，传统主备模式下默认为 `off`。从传统主备迁移至资源池化时，目标表会自动添加 `segment=on` 属性。

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

# 源端 openGauss 配置
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
  # 增量迁移使用的源端 openGauss 逻辑复制槽名称
  slotName: "test_slot"
  # 逻辑复制插件名称（推荐 pgoutput，备选 wal2json）
  pluginName: "pgoutput"
```

## 3 迁移命令

### 3.1 参考命令

完成配置文件修改后，根据所需迁移的对象类型执行对应命令：

```bash
# 迁移表
java -jar openGauss-FullReplicate-7.0.0-RC3.jar --start table --source opengauss --config /path/to/config.yml

# 迁移主键
java -jar openGauss-FullReplicate-7.0.0-RC3.jar --start primarykey --source opengauss --config /path/to/config.yml

# 迁移外键
java -jar openGauss-FullReplicate-7.0.0-RC3.jar --start foreignkey --source opengauss --config /path/to/config.yml

# 迁移索引
java -jar openGauss-FullReplicate-7.0.0-RC3.jar --start index --source opengauss --config /path/to/config.yml

# 迁移约束
java -jar openGauss-FullReplicate-7.0.0-RC3.jar --start constraint --source opengauss --config /path/to/config.yml

# 迁移视图
java -jar openGauss-FullReplicate-7.0.0-RC3.jar --start view --source opengauss --config /path/to/config.yml

# 迁移函数
java -jar openGauss-FullReplicate-7.0.0-RC3.jar --start function --source opengauss --config /path/to/config.yml

# 迁移触发器
java -jar openGauss-FullReplicate-7.0.0-RC3.jar --start trigger --source opengauss --config /path/to/config.yml

# 迁移存储过程
java -jar openGauss-FullReplicate-7.0.0-RC3.jar --start procedure --source opengauss --config /path/to/config.yml

# 迁移序列
java -jar openGauss-FullReplicate-7.0.0-RC3.jar --start sequence --source opengauss --config /path/to/config.yml
```

### 3.2 命令参数说明

| 参数       | 说明                                                 |
| :--------- | :--------------------------------------------------- |
| `--start`  | 指定迁移的对象类型（如 `table`、`index`、`view` 等） |
| `--source` | 指定源数据库类型，openGauss 迁移请使用 `opengauss`   |
| `--config` | 指定配置文件路径                                     |

## 4 迁移规则

### 4.1 数据类型映射

openGauss 到 openGauss 为同构数据库迁移，数据类型完全兼容，不存在类型转换问题，因此不涉及数据类型映射。
