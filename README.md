# debezium

## 介绍
Debezium是一个开源项目，为捕获数据更改(change data capture,CDC)提供了一个低延迟的流式处理平台。

本仓库初始代码来源于Debezium v1.8.1.Final版本

[Debezium v1.8.1.Final](https://github.com/debezium/debezium/tree/v1.8.1.Final)

本仓库构建的目的是为了借助于debezium（需要做一定的适配）和kafka开源组件，构建基于openGauss的在线迁移系统。

在线迁移工具详情请参考

[oracle在线迁移工具](https://gitee.com/opengauss/openGauss-tools-onlineMigration)

[迁移校验工具管理portal](https://gitee.com/opengauss/openGauss-migration-portal)

更多debezium介绍和构建过程请参考

[debezium介绍](https://gitee.com/opengauss/debezium/blob/master/README_ZH.md)

## 构建Debezium

### 软件依赖

使用Debezium代码库并在本地配置它需要以下软件：

- [Git](https://gitee.com/link?target=https%3A%2F%2Fgit-scm.com) 2.2.1 or later
- JDK 11 or later, e.g. [OpenJDK](https://gitee.com/link?target=http%3A%2F%2Fopenjdk.java.net%2Fprojects%2Fjdk%2F)
- [Apache Maven](https://gitee.com/link?target=https%3A%2F%2Fmaven.apache.org%2Findex.html) 3.6.3 or later

有关平台上的安装说明，请参阅上面的链接。您可以通过以下指令查看安装版本

```
git --version
java -version
mvn -version
```

### Debezium oracle connector

构建Debezium oracle connector，需手动下载xstream.jar包，并用如下命令安装至本地maven仓库，详情请参考[Debezium oracle connector](https://gitee.com/opengauss/debezium/tree/master/debezium-connector-oracle)

```
mvn install:install-file -DgroupId=com.oracle.instantclient -DartifactId=xstreams -Dversion=21.1.0.0 -Dpackaging=jar -Dfile=xstreams.jar
```

### Debezium mysql connector

原始debezium mysql connector基于开源组件mysql-binlog-connector-java的0.25.4版本读取binlog，并串行解析为event事件，我们对该开源软件进行修改，可用于支持并行解析event事件，以提高debezium mysql connector 作为source端的性能。

对应的patch文件为[mysql-binlog-connector-java-0.25.4.patch](https://gitee.com/opengauss/debezium/tree/master/debezium-connector-mysql/patch/mysql-binlog-connector-java-0.25.4.patch),并在配置文件中增加参数parallel.parse.event参数控制是否启用并行解析event功能，默认为true，表示启用并行解析能力。

同时针对debezium mysql connector，我们也增加sink端能力，可支持数据在openGauss端按照事务粒度并行回放，用于构建完整的端到端的迁移能力（mysql -> openGauss）。

构建Debezium mysql connector, 需应用patch文件，可利用一键式构建脚本[build.sh](https://gitee.com/opengauss/debezium/tree/master/build.sh)快速编译Debezium mysql connector，编译的压缩包的位置为：

```
debezium-connector-mysql/target/debezium-connector-mysql-1.8.1.Final-plugin.tar.gz
```

### Debezium opengauss connector

我们基于原始的debezium开源软件的debezium postgresql connector，新增了debezium opengauss connector，支持抽取openGauss的逻辑日志，将其存入kafka的topic中。同时，我们增加了debezium opengauss connector的sink端能力，能够抽取kafka的日志并完成数据的回放，以实现对数据dml操作的反向迁移能力（openGauss -> mysql）.
编译debezium后，可以得到反向迁移工具的压缩包，压缩包的位置为：

```
debezium-connector-openngauss/target/debezium-connector-opengauss-1.8.1.Final-plugin.tar.gz
```

### 构建命令

```
mvn clean package -P quick,skip-integration-tests,oracle,jdk11,assembly,xstream,xstream-dependency,skip-tests -Dgpg.skip -Dmaven.test.skip=true
```

## Debezium mysql connector

### 新增功能介绍

原始的debezium mysql connector作为source端，可用于捕获数据变更并存入kafka。现新增如下功能点：

- Source端支持并行解析event事件；
- Source端支持自定义配置快照点；
- 配置gtid_mode=on，source端支持解析last_committed和sequence_num字段，并存入kafka；
- 基于Debezium connector（Kafka Connect）框架，增加sink端能力，可用于从kafka抽取数据并在openGauss端按照事务粒度并行回放。

### 新增配置参数说明

#### Source端

```
connector.class=io.debezium.connector.mysql.MySqlConnector
```

| 参数                            | 类型    | 参数说明                                                   |
| ------------------------------- | ------- | ---------------------------------------------------------- |
| snapshot.offset.binlog.filename | String  | 自定义配置快照点的binlog文件名                             |
| snapshot.offset.binlog.position | String  | 自定义配置快照点的binlog位置                               |
| snapshot.offset.gtid.set        | String  | 自定义配置快照点的Executed_Gtid_Set，需注意最大事务号需减1 |
| parallel.parse.event            | boolean | 是否启用并行解析event能力，默认为true，表示启用并行解析能力         |

```
mysql> show master status;
+------------------+----------+--------------+------------------+-------------------------------------------------+
| File             | Position | Binlog_Do_DB | Binlog_Ignore_DB | Executed_Gtid_Set                               |
+------------------+----------+--------------+------------------+-------------------------------------------------+
| mysql-bin.000027 |   111822 |              |                  | c6eca988-a77e-11ec-8eec-fa163e3d2519:1-50459547 |
+------------------+----------+--------------+------------------+-------------------------------------------------+
1 row in set (0.00 sec)

```

如上示例中，根据查询到的快照点若配置新增的snapshot相关的参数，需配置为如下：

```
snapshot.offset.binlog.filename=mysql-bin.000027
snapshot.offset.binlog.position=111822
snapshot.offset.gtid.set=c6eca988-a77e-11ec-8eec-fa163e3d2519:1-50459546
```

#### Sink端

```
connector.class=io.debezium.connector.mysql.sink.MysqlSinkConnector
```

| 参数                       | 类型   | 参数说明                                                     |
| -------------------------- | ------ | ------------------------------------------------------------ |
| topics                     | String | sink端从kafka抽取数据的topic                                 |
| max.retries                | String | 从kafka抽取数据的最大重试次数                                |
| opengauss.driver           | String | openGauss驱动名                                              |
| opengauss.username         | String | openGauss用户名                                              |
| opengauss.password         | String | openGauss用户密码                                            |
| opengauss.url              | String | openGauss连接url                                             |
| parallel.replay.thread.num | int    | 并行回放默认线程数量，默认为30                               |
| schema.mappings            | String | mysql和openGauss的schema映射关系，与全量迁移chameleon配置相对应，用；区分不同的映射关系，用：区分mysql的database和openGauss的schema<br>例如chameleon的配置<br>schema_mappings:<br/>      mysql_database1: opengauss_schema1<br/>      mysql_database2: opengauss_schema2<br/>则sink端的schema.mappings参数需配置为schema.mappings=mysql_database1:opengauss_schema1;mysql_database2:opengauss_schema2 |

## 基于Debezium mysql connector进行在线迁移

### 环境依赖

kafka，zookeeper，confluent community，debezium-connector-mysql

### 原理

debezium mysql connector的source端，监控mysql数据库的binlog日志，并将数据以AVRO格式写入到kafka；debezium mysql connector的sink端，从kafka读取AVRO格式数据，并组装为事务，在openGauss端按照事务粒度并行回放，从而完成数据从mysql在线迁移至openGauss端。

### 约束及限制

(1) MySQL5.7及以上版本；
(2) MySQL参数配置：

```
binlog_format=row
binglog_row_image=full
gtid_mode=on #若未开启该参数，则sink端按照事务顺序串行回放，会降低在线迁移性能
```
(3) 在线迁移直接透传DDL，对于openGauss和MySQL不兼容的语法，DDL迁移会报错；
(4) Kafka中以AVRO格式存储数据，AVRO字段名称[命名规则](https://avro.apache.org/docs/1.11.1/specification/#names)为：
```
- 以[A-Za-z_]开头
- 随后仅包含[A-Za-z0-9_]
```
因此，对于MySQL中的标识符命名，包括表名、列名等，需满足上述命名规范，否则在线迁移会报错。

### 部署过程

#### 下载依赖

- [kafka](https://mirrors.tuna.tsinghua.edu.cn/apache/kafka/3.2.3/kafka_2.13-3.2.3.tgz)

  ```
  wget -c https://mirrors.tuna.tsinghua.edu.cn/apache/kafka/3.2.3/kafka_2.13-3.2.3.tgz
  
  tar -zxf kafka_2.13-3.2.3.tgz
  ```

- [confluent community](https://packages.confluent.io/archive/5.5/confluent-community-5.5.1-2.12.zip)

  ```
  wget -c  https://packages.confluent.io/archive/5.5/confluent-community-5.5.1-2.12.zip
  
  unzip confluent-community-5.5.1-2.12.zip
  ```

- [debezium-connector-mysql](https://opengauss.obs.cn-south-1.myhuaweicloud.com/latest/tools/debezium-connector-mysql-1.8.1.Final-plugin.tar.gz)

  ```
  wget -c https://opengauss.obs.cn-south-1.myhuaweicloud.com/latest/tools/debezium-connector-mysql-1.8.1.Final-plugin.tar.gz
  
  tar -zxvf debezium-connector-mysql-1.8.1.Final-plugin.tar.gz
  ```

#### 修改配置文件

- zookeeper

  ```
  配置文件位置：/kafka_2.13-3.2.3/config/zookeeper.properties
  ```

- kafka

  ```
  配置文件位置：/kafka_2.13-3.2.3/config/server.properties
  ```

- schema-registry

  ```
  配置文件位置：/confluent-5.5.1/etc/schema-registry/schema-registry.properties
  ```

- connect-standalone

  ```
  配置文件位置：/confluent-5.5.1/etc/schema-registry/connect-avro-standalone.properties
  ```

  注意在plugin.path配置项中增加debezium-connector-mysql所在的路径

  若debezium-connector-mysql所在路径为：/data/debezium_kafka/plugin/debezium-connector-mysql

  则配置plugin.path=share/java,/data/debezium_kafka/plugin

- mysql-source.properties

  ```
  配置文件位置：/confluent-5.5.1/etc/kafka/mysql-source.properties
  ```

  示例详见[mysql-source.properties](https://gitee.com/opengauss/debezium/tree/master/debezium-connector-mysql/patch/mysql-source.properties)

- mysql-sink.properties

  ```
  配置文件位置：/confluent-5.5.1/etc/kafka/mysql-sink.properties
  ```

  示例详见[mysql-sink.properties](https://gitee.com/opengauss/debezium/tree/master/debezium-connector-mysql/patch/mysql-sink.properties)

#### 启动命令

（1）启动zookeeper

```
cd kafka_2.13-3.2.3
./bin/zookeeper-server-start.sh ./config/zookeeper.properties
```

（2）启动kafka

```
cd kafka_2.13-3.2.3
./bin/kafka-server-start.sh ./config/server.properties
```

（3）注册schema

```
cd confluent-5.5.1
./bin/schema-registry-start etc/schema-registry/schema-registry.properties
```

（4）启动kafka-connect source端

```
cd confluent-5.5.1
./bin/connect-standalone etc/schema-registry/connect-avro-standalone.properties etc/kafka/mysql-source.properties
```

（5）启动kafka-connect sink端

```
cd confluent-5.5.1
./bin/connect-standalone etc/schema-registry/connect-avro-standalone-1.properties etc/kafka/mysql-sink.properties
```
说明：source端和sink端的两个配置文件connect-avro-standalone.properties和connect-avro-standalone-1.properties的差异点在于rest.port参数的不同，默认为8083，即两个文件中设置不同的端口号，即可启动多个kafka-connect，实现sink端和source端独立工作。

其他命令：

（1）查看topic

```
cd kafka_2.13-3.2.3
./bin/kafka-topics.sh --bootstrap-server 127.0.0.1:9092 --list
```

（2）查看topic的内容

```
cd confluent-5.5.1
./bin/kafka-avro-console-consumer --bootstrap-server 127.0.0.1:9092 --topic topic_name --from-beginning
```

## Debezium opengauss connector

### 功能介绍

新增的debezium opengauss connector作为source端，可用于捕获数据变更并存入kafka。在此基础上添加sink端功能，功能点如下：

- 支持openGauss端对schema下的数据的dml操作同步到MySQL端，不支持迁移ddl操作的迁移；
- Sink端支持数据按表进行并发回放；
- 支持openGausss的多个schema下的数据迁移到指定的MySQL的多个库。

### 新增配置参数说明

#### Sink端

```
connector.class=io.debezium.connector.opengauss.sink.OpengaussSinkConnector
```

| 参数                         | 类型   | 参数说明                                                                                                                                                                                                                                                                                                                                               |
|----------------------------| ------ |----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| topics                     | String | sink端从kafka抽取数据的topic                                                                                                                                                                                                                                                                                                                              |
| max.retries                | String | 从kafka抽取数据的最大重试次数                                                                                                                                                                                                                                                                                                                                  |
| max_thread_count           | String | 自定义Sink端按表回放时的最大并发线程数（不能为0）                                                                                                                                                                                                                                                                                                                        |
| mysql.username             | String | MySQL用户名                                                                                                                                                                                                                                                                                                                                           |
| mysql.password             | String | MySQL用户密码                                                                                                                                                                                                                                                                                                                                          |
| masql.url                  | String | MySQL连接url                                                                                                                                                                                                                                                                                                                                         |
| schema.mappings            | String | openGauss的schema与MySQL的映射关系，与全量迁移chameleon配置相反，用；区分不同的映射关系，用：区分openGauss的schema与MySQL的database<br>例如chameleon的配置<br>schema_mappings:<br/>      mysql_database1: opengauss_schema1<br/>      mysql_database2: opengauss_schema2<br/>则sink端的schema.mappings参数需配置为schema.mappings=opengauss_schema1:mysql_database1;opengauss_schema2:mysql_database2 |



## 基于Debezium opengauss connector进行反向迁移

### 环境依赖

kafka， zookeeper，confluent community，debezium-connector-opengauss

### 原理

debezium opengauss connector的source端，监控openGauss数据库的逻辑日志，并将数据写入到kafka；debezium opengauss connector的sink端，从kafka读取数据，并组装为sql语句，在MySQL端按表并行回放，从而完成数据从openGauss在线迁移至MySQL端。

### 前置条件

openGauss开启逻辑复制功能：

    （1）仅限初始用户和拥有REPLICATION权限的用户进行操作。三权分立关闭时数据库管理员可以进行逻辑复制操作，三权分立开启时不允许数据库管理员进行逻辑复制操作。
    （2）openGauss的库与逻辑复制槽一一对应，当待迁移的库改变时，需要配置新的逻辑复制槽的名字
    （3）openGauss开启逻辑复制槽后，对于没有主键的表，不能直接进行update和delete操作，需要先执行命令：ALTER TABLE table_name REPLICA IDENTITY FULL;

openGauss参数配置：

```
ssl=on
wal_level=logical
```

### 部署过程

#### 下载依赖

- [kafka](https://mirrors.tuna.tsinghua.edu.cn/apache/kafka/3.2.3/kafka_2.13-3.2.3.tgz)

  ```
  wget -c https://mirrors.tuna.tsinghua.edu.cn/apache/kafka/3.2.3/kafka_2.13-3.2.3.tgz
  
  tar -zxf kafka_2.13-3.2.3.tgz
  ```

- [confluent community](https://packages.confluent.io/archive/5.5/confluent-community-5.5.1-2.12.zip)

  ```
  wget -c  https://packages.confluent.io/archive/5.5/confluent-community-5.5.1-2.12.zip
  
  unzip confluent-community-5.5.1-2.12.zip
  ```

- [debezium-connector-opengauss](https://opengauss.obs.cn-south-1.myhuaweicloud.com/latest/tools/debezium-connector-opengauss-1.8.1.Final-plugin.tar.gz)

  ```
  wget -c https://opengauss.obs.cn-south-1.myhuaweicloud.com/latest/tools/debezium-connector-opengauss-1.8.1.Final-plugin.tar.gz
  
  tar -zxvf debezium-connector-opengauss-1.8.1.Final-plugin.tar.gz
  ```

#### 修改配置文件

- zookeeper

  ```
  配置文件位置：/kafka_2.13-3.2.3/config/zookeeper.properties
  ```

- kafka

  ```
  配置文件位置：/kafka_2.13-3.2.3/config/server.properties
  ```

- schema-registry

  ```
  配置文件位置：/confluent-5.5.1/etc/schema-registry/schema-registry.properties
  ```

- connect-standalone

  ```
  配置文件位置：/confluent-5.5.1/etc/schema-registry/connect-avro-standalone.properties
  ```

  注意在plugin.path配置项中增加debezium-connector-opengauss所在的路径

  若debezium-connector-opengauss所在路径为：/data/debezium_kafka/plugin/debezium-connector-opengauss

  则配置plugin.path=share/java,/data/debezium_kafka/plugin

- opengauss-source.properties

  ```
  配置文件位置：/confluent-5.5.1/etc/kafka/opengauss-source.properties
  ```

  示例详见[opengauss-source.properties](https://gitee.com/opengauss/debezium/blob/master/debezium-connector-opengauss/patch/opengauss-source.properties)

- opengauss-sink.properties

  ```
  配置文件位置：/confluent-5.5.1/etc/kafka/opengauss-sink.properties
  ```

  示例详见[opengauss-sink.properties](https://gitee.com/opengauss/debezium/blob/master/debezium-connector-opengauss/patch/opengauss-sink.properties)

#### 启动命令

（1）启动zookeeper

```
cd kafka_2.13-3.2.3
./bin/zookeeper-server-start.sh ./config/zookeeper.properties
```

（2）启动kafka

```
cd kafka_2.13-3.2.3
./bin/kafka-server-start.sh ./config/server.properties
```

（3）注册schema

```
cd confluent-5.5.1
./bin/schema-registry-start etc/schema-registry/schema-registry.properties
```

（4）启动kafka-connect source端

```
cd confluent-5.5.1
./bin/connect-standalone etc/schema-registry/connect-avro-standalone.properties etc/kafka/opengauss-source.properties
```

（5）启动kafka-connect sink端

```
cd confluent-5.5.1
./bin/connect-standalone etc/schema-registry/connect-avro-standalone-1.properties etc/kafka/opengauss-sink.properties
```

说明：source端和sink端两个配置文件connect-avro-standalone.properties和connect-avro-standalone-1.properties的差异点在于rest.port参数的不同，默认为8083，即两个文件中设置不同的端口号，即可启动多个kafka-connect，实现source端和sink端独立工作。

其他命令：

（1）查看topic

```
cd kafka_2.13-3.2.3
./bin/kafka-topics.sh --bootstrap-server 127.0.0.1:9092 --list
```

（2）查看topic的内容

```
cd confluent-5.5.1
./bin/kafka-avro-console-consumer --bootstrap-server 127.0.0.1:9092 --topic topic_name --from-beginning
```

#### 性能测试模型

| 场景                  | 数据量                        | 性能      |
|---------------------|----------------------------|---------|
| insert.lua          | 30线程，30张表，每张表1000行数据，50秒   | 3W+ tps |
| update_index.lua    | 30线程，30张表，每张表10000行数据，50秒  | 2W+ tps |
| update_non_index.lua | 30线程，30张表，每张表10000行数据，50秒  | 2W+ tps |
| delete.lua          | 30线程，30张表，每张表100000行数据，5秒  | 3W+ tps |
| 混合场景                | 50线程，50张表，每张表100000行数据，50秒 | 3W+ tps |
