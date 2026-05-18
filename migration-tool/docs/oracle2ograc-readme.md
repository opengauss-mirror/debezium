# 1. 概述

## 1.1 目的

本文旨在对openGauss-FullReplicate工具进行介绍，指导用户如何完成工具安装、并使用工具完成数据迁移，具体支持迁移的迁移类型如下：

- 从 Oracle 迁移至 oGRAC

## 1.2 openGauss-FullReplicate工具介绍

openGauss-FullReplicate是一个用Java编写的数据迁移工具。该工具提供了全量数据和对象的迁移能力，全量数据迁移采用多表并行迁移，
全量对象支持表、约束、索引、外键、视图、函数、触发器、存储过程和序列的迁移。

## 1.3 注意事项
### 1.3.1 一般性限制

- 创建 oGRAC 目标端数据库时，需要指定数据库编码格式与源端一致,并确保源端与目标端时区一致性

### 1.3.2 对象迁移限制

- 由于内核兼容性在持续增强，对象迁移采用先透传再翻译的原则进行，即先直接透传对象创建语句在 oGRAC 端执行，若执行失败，再借助开源三方件druid进行翻译。

### 1.3.3 Oracle迁移限制

- 要求Oracle版本为19
- 虚拟列（Virtual Column）在迁移时会自动过滤，不迁移到目标端
- 引用分区表（基于外键关系分区）暂不支持迁移
- 系统分区表（应用程序控制分区）暂不支持迁移
- 虚拟列分区表（基于虚拟列分区）暂不支持迁移
- 函数索引（Function-based Index）仅支持部分函数表达式
- 几何类型（geometry和geography）暂不支持迁移
- 视图、函数、触发器和存储过程目前仅支持迁移流程，迁移成功还需语法兼容

### 1.3.4 迁移前准备

为了确保迁移过程的效率和准确性，建议在迁移前执行以下操作：

1. **更新Oracle表统计信息**：执行以下命令更新指定schema的表统计信息，这将有助于DataX生成更优的执行计划：

   ```sql
   EXEC DBMS_STATS.GATHER_SCHEMA_STATS('YOUR_SCHEMA_NAME', cascade=>TRUE);
   SELECT table_name, num_rows, last_analyzed FROM user_tables;
   ```

   其中 `YOUR_SCHEMA_NAME` 是您要迁移的Oracle schema名称。

2. **检查表空间使用情况**：确保目标端oGRAC数据库有足够的表空间用于迁移操作。

# 2. 安装方法

## 2.1 安装环境要求

由于工具使用Java编写，因此需要提前安装Java运行环境，版本要求Java 17+。

## 2.2 安装包下载

安装包下载地址：https://opengauss.obs.cn-south-1.myhuaweicloud.com/latest/tools/openGauss-FullReplicate-7.0.0-RC3.tar.gz
其中7.0.0-RC3表示当前版本号。

```bash
wget https://opengauss.obs.cn-south-1.myhuaweicloud.com/latest/tools/openGauss-FullReplicate-7.0.0-RC3.tar.gz
```

## 2.3 安装包解压

下载完成后，解压压缩包。

```
tar -zxvf openGauss-FullReplicate-7.0.0-RC3.tar.gz
```

解压后参考目录如下：

```text
openGauss-FullReplicate/
openGauss-FullReplicate/config/
openGauss-FullReplicate/config/config.yml
openGauss-FullReplicate/build_commit_id.log
openGauss-FullReplicate/openGauss-FullReplicate-7.0.0-RC3.jar
```

其中openGauss-FullReplicate-7.0.0-RC3.jar为工具的主程序，config文件夹下为配置文件模板。

# 3. 配置文件说明
配置文件使用yaml文件规则配置，需要特别注意对齐，缩进表示层级关系，缩进时不允许使用Tab键，只允许使用空格，缩进的空格数目不重要，但相同层级的元素左侧需要对齐。

```yaml
# global settings
# 是否记录进度
isDumpJson: true
# 进度文件地址
statusDir: ./process
# 目标数据库类型，如：opengauss, ograc
targetType: ograc
# 目标端数据库配置
ogConn:
  host: "192.168.0.2"
  port: 1611
  user: "username"
  password: "password"
  database: "database"
  charset: "utf8"
  params:
sourceConfig:
  # 查询表的线程数
  readerNum: 4
  # 写表的线程数
  writerNum: 4
  # 线程队列容量
  threadQueueCapacity: 20000
  # 源端数据库连接信息
  dbConn:
    host: "192.168.0.1"
    port: "1521"
    user: "username"
    password: "password"
    database: "ORCL"
    charset: 'utf8'
    connectTimeout: 10
  # schema映射关系
  schemaMappings:
    SCOTT: ogtest
datax:
  dataxHome: datax
  enableKeepDataXTemporaryConfig: true
  enableOutputDataxLogs: false
```
## 配置参数详细信息

### 全局配置参数

| 参数名 | 类型 | 必填 | 默认值 | 描述 |
| :--- | :--- | :--- | :--- | :--- |
| `isDumpJson` | Boolean | 是 | - | 是否记录进度 |
| `statusDir` | String | 否 | - | 进度文件地址 |
| `targetType` | String | 否 | - | 目标数据库类型，如：opengauss, ograc |
| `ogConn` | Object | 是 | - | OGRAC数据库连接配置 |
| `sourceConfig` | Object | 是 | - | 源数据库配置 |
| `datax` | Object | 否 | - | DataX配置 |

### 数据库连接配置 (DatabaseConfig)

| 参数名 | 类型 | 必填 | 默认值 | 描述 |
| :--- | :--- | :--- | :--- | :--- |
| `host` | String | 是 | - | 数据库主机地址 |
| `port` | Integer | 是 | - | 数据库端口 |
| `user` | String | 是 | - | 数据库用户名 |
| `password` | String | 是 | - | 数据库密码 |
| `database` | String | 是 | - | 数据库名称 |
| `charset` | String | 否 | - | 字符集 |
| `connectTimeout` | Integer | 否 | - | 连接超时时间（秒） |
| `params` | Object | 否 | - | 其他连接参数 |

### 源数据库配置 (SourceConfig)

| 参数名 | 类型 | 必填 | 默认值 | 描述 |
| :--- | :--- | :--- | :--- | :--- |
| `readerNum` | Integer | 是 | - | 查询表的线程数 |
| `writerNum` | Integer | 是 | - | 写表的线程数 |
| `threadQueueCapacity` | Integer | 否 | - | 线程队列容量 |
| `dbConn` | Object | 是 | - | 源端数据库连接信息 |
| `schemaMappings` | Object | 是 | - | schema映射关系 |

### DataX配置 (DataXParamConfig)

| 参数名 | 类型 | 必填 | 默认值 | 描述 |
| :--- | :--- | :--- | :--- | :--- |
| `dataxHome` | String | 否 | - | DataX主目录 |
| `readerName` | String | 否 | - | 读取器名称 |
| `writerName` | String | 否 | - | 写入器名称 |
| `channel` | Integer | 否 | - | 通道数 |
| `errorRecordLimit` | Integer | 否 | - | 错误记录限制 |
| `errorPercentageLimit` | Double | 否 | - | 错误百分比限制 |
| `readBatchSize` | Integer | 否 | - | 读取批大小 |
| `readTimeout` | Integer | 否 | - | 读取超时 |
| `writeBatchSize` | Integer | 否 | - | 写入批大小 |
| `writeTimeout` | Integer | 否 | - | 写入超时 |
| `enableBatchWrite` | Boolean | 否 | - | 是否启用批写入 |
| `enablePrepareStatement` | Boolean | 否 | - | 是否启用预处理语句 |
| `batchWriteSize` | Integer | 否 | - | 批写入大小 |
| `retryTimes` | Integer | 否 | - | 重试次数 |
| `retryInterval` | Integer | 否 | - | 重试间隔 |
| `enableKeepDataXTemporaryConfig` | Boolean | 否 | false | 是否保留DataX临时配置 |
| `enableOutputDataxLogs` | Boolean | 否 | false | 是否输出DataX日志 |

# 4. 迁移命令

完成配置文件配置后，即可开始迁移，迁移命令参考如下：

其中， --start参数为迁移的对象类型，--source参数为源端数据库类型，支持oracle，--config参数为配置文件路径。

迁移命令不支持并行执行（同时执行表，索引等迁移命令）

## 4.1 Oracle迁移命令

```bash
# 迁移表
java -jar openGauss-FullReplicate-7.0.0-RC3.jar --start datax_table --source oracle --config /**/**/config.yml

# 迁移主键
java -jar openGauss-FullReplicate-7.0.0-RC3.jar --start primarykey --source oracle --config /**/**/config.yml

# 迁移外键
java -jar openGauss-FullReplicate-7.0.0-RC3.jar --start foreignkey --source oracle --config /**/**/config.yml

# 迁移索引
java -jar openGauss-FullReplicate-7.0.0-RC3.jar --start index --source oracle --config /**/**/config.yml

# 迁移约束
java -jar openGauss-FullReplicate-7.0.0-RC3.jar --start constraint --source oracle --config /**/**/config.yml

# 迁移视图
java -jar openGauss-FullReplicate-7.0.0-RC3.jar --start view --source oracle --config /**/**/config.yml

# 迁移函数
java -jar openGauss-FullReplicate-7.0.0-RC3.jar --start function --source oracle --config /**/**/config.yml

# 迁移触发器
java -jar openGauss-FullReplicate-7.0.0-RC3.jar --start trigger --source oracle --config /**/**/config.yml

# 迁移存储过程
java -jar openGauss-FullReplicate-7.0.0-RC3.jar --start procedure --source oracle --config /**/**/config.yml

# 迁移序列
java -jar openGauss-FullReplicate-7.0.0-RC3.jar --start sequence --source oracle --config /**/**/config.yml
```

# 5. 默认的类型转换规则

## 5.1 列类型转换

| Oracle           | oGRAC                          | 备注                                         |
|:-----------------|:-------------------------------|:-------------------------------------------|
| **数值类型**     |                                |                                            |
| NUMBER           | NUMBER                         | 当长度为38或<=0时保持原类型                           |
| NUMBER           | BIGINT/BINARY_BIGINT           | 自增类型                           |
| NUMBER(p)        | NUMBER(p)                      | 当长度>0且<38时包含长度信息                           |
| FLOAT            | DECIMAL(38, length)            | 转换为DECIMAL(38, length)                     |
| BINARY_FLOAT     | BINARY_FLOAT                   | 类型名称一致                                     |
| BINARY_DOUBLE    | BINARY_DOUBLE                  | 类型名称一致                                     |
| DOUBLE PRECISION | DOUBLE PRECISION               | 类型名称一致                                     |
| **字符串类型**   |                                |                                            |
| CHAR(n)          | CHAR(n)                        | 保持原类型，包含长度信息，如有CHAR_USED信息则添加              |
| VARCHAR2(n)      | VARCHAR2(n)                    | 保持原类型，包含长度信息，如有CHAR_USED信息则添加              |
| NCHAR(n)         | NCHAR(n)                       | 保持原类型，包含长度信息                               |
| NVARCHAR2(n)     | NVARCHAR2(n)                   | 保持原类型，包含长度信息                               |
| CLOB             | CLOB                           | 类型名称一致                                     |
| NCLOB            | CLOB                           | 转换为CLOB                                    |
| **大对象类型**   |                                |                                            |
| BLOB             | BLOB                           | 类型名称一致                                     |
| NBLOB            | BLOB                           | 转换为BLOB                                    |
| RAW(n)           | RAW(n)                         | 保持原类型，包含长度信息                               |
| LONG RAW         | BLOB                           | 转换为BLOB                                    |
| BFILE            |                                | 不兼容，抛出异常                                   |
| **日期时间类型** |                                |                                            |
| DATE             | DATE                           | 类型名称一致                                     |
| TIMESTAMP        | TIMESTAMP                      | 类型名称一致                                     |
| TIMESTAMP(p)     | TIMESTAMP(p)                   | 当精度<=6时保持原类型                               |
| TIMESTAMP(p)     | TIMESTAMP(6)                   | 当精度>6时转换为TIMESTAMP(6)                      |
| TIMESTAMP WITH TIME ZONE | TIMESTAMP WITH TIME ZONE | 类型名称一致                                     |
| TIMESTAMP(p) WITH TIME ZONE | TIMESTAMP(p) WITH TIME ZONE | 当精度<=6时保持原类型                               |
| TIMESTAMP(p) WITH TIME ZONE | TIMESTAMP(6) WITH TIME ZONE | 当精度>6时转换为TIMESTAMP(6) WITH TIME ZONE       |
| TIMESTAMP WITH LOCAL TIME ZONE | TIMESTAMP WITH LOCAL TIME ZONE | 类型名称一致                                     |
| TIMESTAMP(p) WITH LOCAL TIME ZONE | TIMESTAMP(p) WITH LOCAL TIME ZONE | 当精度<=6时保持原类型                               |
| TIMESTAMP(p) WITH LOCAL TIME ZONE | TIMESTAMP(6) WITH LOCAL TIME ZONE | 当精度>6时转换为TIMESTAMP(6) WITH LOCAL TIME ZONE |
| INTERVAL YEAR TO MONTH | INTERVAL YEAR TO MONTH   | 类型名称一致                                     |
| INTERVAL YEAR(n) TO MONTH | INTERVAL YEAR(4) TO MONTH   | 当长度>4时转换为INTERVAL YEAR(4) TO MONTH         |
| INTERVAL DAY TO SECOND | INTERVAL DAY TO SECOND   | 类型名称一致                                     |
| INTERVAL DAY(n) TO SECOND(m) | INTERVAL DAY(n) TO SECOND(m) | 当长度<=6且精度<=6时保持原类型                         |
| INTERVAL DAY(n) TO SECOND(m) | INTERVAL DAY(6) TO SECOND(6) | 当长度>6或精度>6时转换为INTERVAL DAY(6) TO SECOND(6) |
| **特殊类型**     |                                |                                            |
| XMLTYPE          |                                | 转换为Clob                                    |
| JSON             |                                | 不兼容，抛出异常                                   |
| ANYDATA          |                                | 不兼容，抛出异常                                   |

## 5.2 索引类型转换

| Oracle              | oGRAC              | 备注                                         |
|:--------------------|:-------------------|:---------------------------------------------|
| **标准索引**        |                    |                                              |
| B-tree Index        | B-tree Index       | 完全兼容                                     |
| Unique Index        | Unique Index       | 完全兼容                                     |
| Non-Unique Index    | B-tree Index       | 完全兼容                                     |
| **特殊索引**        |                    |                                              |
| Reverse Key Index   | B-tree Index       | 完全兼容      |
| Function-based Index| Function Index     | 部分兼容，仅支持特定函数                     |
| Composite Index     | Composite Index    | 完全兼容，复合索引最多支持16列               |
| Bitmap Index        | B-tree Index       | 转为普通索引            |
| **不支持的索引**    |                    |                                              |
| Full-Text Index     | -                  | 不兼容，Oracle Text索引                      |
| Domain Index        | -                  | 不兼容，如CTXSYS.CONTEXT                     |
| Spatial Index       | Gist Index         | 不兼容                      |
| XML Index           | -                  | 不兼容                                       |
| Filtered Index      | Partial Index      | 不兼容             |


### 5.2.1 函数索引支持的函数列表

oGRAC函数索引仅支持以下函数表达式：

| 函数名         | 说明                      | 示例                              |
|:---------------|:--------------------------|:----------------------------------|
| ABS            | 绝对值                    | ABS(num_col)                      |
| CHARTOROWID    | 字符串转ROWID             | CHARTOROWID(rowid_str_col)        |
| DECODE         | 条件判断                  | DECODE(int_col, 0, 'ZERO', 1, 'ONE', 'OTHER') |
| LOWER          | 转小写                    | LOWER(char_col)                   |
| NVL            | 空值替换                  | NVL(nullable_col, 0)              |
| NVL2           | 空值条件替换              | NVL2(nullable_col, 1, 0)          |
| REGEXP_INSTR   | 正则表达式匹配位置        | REGEXP_INSTR(text_col, 'word')    |
| REGEXP_SUBSTR  | 正则表达式提取子串        | REGEXP_SUBSTR(text_col, '[a-zA-Z]+') |
| REVERSE        | 字符串反转                | REVERSE(char_col)                 |
| SUBSTR         | 字符串截取                | SUBSTR(text_col, 1, 20)           |
| SUBSTRB        | 字节截取                  | SUBSTRB(text_col, 1, 20)          |
| TO_CHAR        | 转字符串                  | TO_CHAR(date_col)                 |
| TO_DATE        | 转日期                    | TO_DATE(TO_CHAR(date_col, 'yyyy-mm-dd'), 'yyyy-mm-dd') |
| TO_NUMBER      | 转数字                    | TO_NUMBER(TO_CHAR(num_col))       |
| TRIM           | 去除空格                  | TRIM(text_col)                    |
| TRUNC          | 数字截断                  | TRUNC(num_col)                    |
| TRUNC          | 日期截断                  | TRUNC(date_col, 'yyyy')           |
| UPPER          | 转大写                    | UPPER(char_col)                   |

### 5.2.2 函数索引不支持的函数列表

oGRAC函数索引不支持以下函数表达式用法：

| 函数名                | 说明              | 原因              |
|:----------------------|:------------------|:------------------|
| `upper('constant')`   | 常量转大写        | 不支持常量表达式  |
| `nvl(c_text,c_test2)` | 多参数空值替换    | 暂不支持          |
| `upper(c_json_lob)`   | JSON LOB转大写    | 不支持LOB类型     |
| `nvm(c_arr,c_arr)`    | 数组空值替换      | 不支持数组类型    |

### 5.3 自增主键迁移
   colName NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY ->  colName BIGINT AUTO_INCREMENT NOT NULL PRIMARY KEY
   colName NUMBER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY ->  colName BIGINT AUTO_INCREMENT NOT NULL PRIMARY KEY
   ograc数据库自增键约束，必须为int 类型，且为主键/唯一键
   ograc自增类型 SERIAL PRIMARY KEY/AUTO_INCREMENT NOT NULL PRIMARY KEY 对应 GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY
   ograc自增类型不支持oracle的identity列ALWAYS模式，同步转换为AUTO_INCREMENT

### 5.4 分区表迁移 
   分区表迁移，分区对应表空间，如果不是默认表空间，则需要手动创建分区对应同名表空间

### # 6. 测试场景执行操作

我们在migration-tool-test项目中提供了Oracle2oGRAC测试场景预制与清理功能，具体使用方法请参考项目文档。https://gitcode.com/opengauss/debezium/blob/master/migration-tool-test/README.md

# 7 迁移建议

### 7.1 执行机器配置要求

为了确保迁移工具的正常运行和最佳性能，执行机器需要满足以下配置要求：

#### 7.1.1 硬件配置

| 配置项 | 最低要求 | 推荐配置 | 说明 |
|--------|---------|---------|------|
| CPU | 4核 | 8核及以上 | 并发处理能力，影响多表并行迁移速度 |
| 内存 | 8GB | 16GB及以上 | 用于JVM运行和DataX处理 |
| 磁盘空间 | 50GB | 100GB及以上 | 用于存放工具、DataX、临时文件和日志 |

#### 7.1.2 内存大小约束

根据迁移数据量和表大小，执行机器需要足够的内存来支持JVM和DataX的运行：

1. **JVM内存约束**：
   - 工具本身需要至少2GB内存
   - DataX根据表大小自动调整JVM参数，最大可能需要4GB内存
   - 并发迁移多个大表时，内存需求会增加

2. **内存使用估算**：
   - 小数据量迁移（<100万行）：8GB内存足够
   - 中等数据量迁移（100万-1000万行）：16GB内存推荐
   - 大数据量迁移（>1000万行）：32GB内存推荐

3. **内存配置建议**：
   - 执行迁移命令时，可通过 `-Xmx` 参数调整JVM最大内存
   - 示例：`java -Xmx8g -jar openGauss-FullReplicate-7.0.0-RC3.jar --start datax_table --source oracle --config config.yml`
   - 确保执行机器有足够的物理内存，避免使用过多交换空间

#### 7.1.3 操作系统要求

- **操作系统**：Linux (推荐) 或 Windows
- **文件系统**：建议使用SSD存储，提高临时文件读写速度
- **网络**：源数据库和目标数据库之间的网络带宽至少1Gbps，延迟<10ms

### 7.2 并发度配置

当前配置文件默认并发度为4个线程查询表和4个线程写表：

- **查询表的线程数 (readerNum): 4**
- **写表的线程数 (writerNum): 4**

这些配置决定了迁移过程中同时处理的表数量，影响整体迁移速度和系统资源占用。

### 7.2 DataX配置策略

迁移工具使用 `GeneralDataXConfigStrategy` 作为DataX的配置策略，主要特点如下：

#### 7.2.1 动态Channel配置

根据表大小自动调整DataX的Channel数量：

| 表行数 | Channel数 |
|--------|-----------|
| ≤10,000 | 1 |
| 10,000-100,000 | 2 |
| 100,000-1,000,000 | 4 |
| >1,000,000 | 最多8个，或CPU核心数的一半 |

#### 7.2.2 JVM参数配置

根据表大小自动调整JVM参数：

| 表行数 | JVM参数 |
|--------|---------|
| ≤10,000 | -Xms512m -Xmx512m |
| 10,000-1,000,000 | -Xms1g -Xmx1g |
| 1,000,000-10,000,000 | -Xms2g -Xmx2g |
| >10,000,000 | -Xms4g -Xmx4g |

#### 7.2.3 批处理大小配置

根据表大小自动调整批处理大小：

| 表行数 | 批处理大小 |
|--------|------------|
| ≤10,000 | 500 |
| 10,000-1,000,000 | 1,000 |
| 1,000,000-10,000,000 | 2,000 |
| >10,000,000 | 4,000 |

#### 7.2.4 分片策略

- **有单一主键的表**：使用主键作为分片键
- **无主键或多主键的表**：使用DataX-OracleReader的默认分片策略，根据表大小自动调整分片数量

#### 7.2.5 根据最大表与并发度评估迁移工具的内存占用

迁移工具预估内存大小计算

1. **基础内存需求**
   - 迁移工具本身：至少2GB内存

2. **DataX任务内存需求**
   - 根据并发度参数（writerNum: 4），最多同时运行4个DataX任务，每个任务的内存需求如下：
   
   | 表大小 | 单个DataX任务内存 | 4个任务最大内存 |
   |--------|------------------|----------------|
   | ≤10,000行 | 512MB | 2GB |
   | 10,000-1,000,000行 | 1GB | 4GB |
   | 1,000,000-10,000,000行 | 2GB | 8GB |
   | >10,000,000行 | 4GB | 16GB |

3. **总内存需求**
   - **最小预估**：工具本身(2GB) + 4个小表(2GB) = 4GB
   - **中等预估**：工具本身(2GB) + 4个中等表(4GB) = 6GB
   - **最大预估**：工具本身(2GB) + 4个超大表(16GB) = 18GB

4. **实际建议**
   - **小数据量迁移**（主要是小表）：8GB内存足够
   - **中等数据量迁移**（包含中等表）：16GB内存推荐
   - **大数据量迁移**（包含大表或超大表）：32GB内存推荐

这些预估基于迁移工具的配置参数，实际使用时应根据具体的表大小分布和服务器资源情况进行调整。
