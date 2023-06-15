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
- 配置gtid_mode=on，source端支持解析last_committed和sequence_number字段，并存入kafka；
- 基于Debezium connector（Kafka Connect）框架，增加sink端能力，可用于从kafka抽取数据并在openGauss端按照事务粒度并行回放;
- 增加迁移进度上报功能，可用于读取数据迁移时延。

### 新增配置参数说明

#### Source端

(1) 启动类

```
connector.class=io.debezium.connector.mysql.MySqlConnector
```

(2) 配置文件示例

[mysql-source.properties](https://gitee.com/opengauss/debezium/tree/master/debezium-connector-mysql/patch/mysql-source.properties)

(3) debezium原生参数含义请参考：

[debezium原生参数](https://debezium.io/documentation/reference/1.8/connectors/mysql.html)

(4) topic路由请参考：

[topic路由](https://debezium.io/documentation/reference/stable/transformations/topic-routing.html)

在线迁移方案严格保证事务的顺序性，因此将DDL和DML路由在kafka的一个topic下，且该topic的分区数只能为1(参数num.partitions=1)，从而保证source端推送到kafka，和sink端从kafka拉取数据都是严格保序的。

默认情况下，debezium mysql connector针对DDL，DML和事务创建独立的topic，且每个表为一个topic

事务topic需配置provide.transaction.metadata=true，才显式生成事务topic

topic命名规则为：

DDL topic名称：${database.server.name}

DML topic名称：${database.server.name}.db_name.table_name

事务topic名称：${database.server.name}.transaction

DDL，DML和事务topic名称均以${database.server.name}开头，因此以前缀方式去正则匹配合并topic，将DDL，DML和事务的
topic进行路由合并为一个topic，且该topic的分区数只能为1。

source端将数据推送至该topic下，同时sink端配置topics为合并后的topic，用于从kafka抽取数据，从而可保证事务的顺序。

DDL，DML和事务topic利用路由转发功能进行合并的配置如下：

Source端：
```
database.server.name=mysql_server

provide.transaction.metadata=true

transforms=route
transforms.route.type=org.apache.kafka.connect.transforms.RegexRouter
transforms.route.regex=^mysql_server(.*)
transforms.route.replacement=mysql_server_topic
```

Sink端：
```
topics=mysql_server_topic
```

(5) 新增配置参数说明

| 参数          | 类型      | 参数说明                                                                |
|-------------|---------|---------------------------------------------------------------------|
| snapshot.offset.binlog.filename | String  | 自定义配置快照点的binlog文件名                                                  |
| snapshot.offset.binlog.position | String  | 自定义配置快照点的binlog位置                                                   |
| snapshot.offset.gtid.set | String  | 自定义配置快照点的Executed_Gtid_Set，需注意最大事务号需减1                              |
| parallel.parse.event | boolean | 是否启用并行解析event能力，默认为true，表示启用并行解析能力                                  |
| commit.process.while.running | boolean | 是否开启迁移进度上报功能，默认为false，表示不开启该功能                                      |
| source.process.file.path | String  | 迁移进度文件输出路径，默认在迁移插件同一目录下，在迁移进度上报功能开启后起作用                             |
| commit.time.interval | int     | 迁移进度上报的时间间隔，默认值为1，单位：秒，在迁移进度上报功能开启后起作用                              |
| create.count.info.path | String  | 源端binlog日志的事务号输出路径，默认在迁移插件同一目录下，必须与sink端的该路径保持一致，用于和sink端交互获取总体同步时延 |
| process.file.count.limit| int     | 同一目录下文件数目限制，超过该数目工具会按时间从早到晚删除多余进度文件，默认为10                           |
| process.file.time.limit | int     | 进度文件保存时间，超过该时间后工具会删除对应的进度文件，默认为168，单位：小时                            |
| append.write | boolean | 进度文件写入方式，true表示追加写入，false表示覆盖写入，默认值为false                           |
| file.size.limit | int     | 文件大小限制，超过该限制值工具会另启新文件写入，默认为10，单位：兆                                  |

快照点参数配置说明：

case 1: 查询到一个gtid set，则配置当前查询的gtid set，且最大事务号减1。

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

case 2：查询到多个gtid set，则配置所有gtid，并针对当前uuid对应的gtid set，设置最大事务号减1。

```
mysql> show master status;
+------------------+----------+--------------+------------------+---------------------------------------------------------------------------------------------+
| File             | Position | Binlog_Do_DB | Binlog_Ignore_DB | Executed_Gtid_Set                                                                           |
+------------------+----------+--------------+------------------+---------------------------------------------------------------------------------------------+
| mysql-bin.000034 |    15973 |              |                  | a3ea3aee-ab76-11ed-9e33-fa163e3d2519:1-297,
c6eca988-a77e-11ec-8eec-fa163e3d2519:1-50459811 |
+------------------+----------+--------------+------------------+---------------------------------------------------------------------------------------------+
1 row in set (0.00 sec)
```

查询当前server_uuid

```
mysql> show global variables like 'server_uuid';
+---------------+--------------------------------------+
| Variable_name | Value                                |
+---------------+--------------------------------------+
| server_uuid   | a3ea3aee-ab76-11ed-9e33-fa163e3d2519 |
+---------------+--------------------------------------+
1 row in set (0.00 sec)
```

如上示例中，根据查询到的快照点若配置新增的snapshot相关的参数，需配置为如下：

```
snapshot.offset.binlog.filename=mysql-bin.000034
snapshot.offset.binlog.position=15973
snapshot.offset.gtid.set=a3ea3aee-ab76-11ed-9e33-fa163e3d2519:1-296,c6eca988-a77e-11ec-8eec-fa163e3d2519:1-50459811
```

case 3：若和全量迁移chameleon对接，快照点从表sch_chameleon.t_replica_batch中获得。

binlog文件名与列t_binlog_name相对应，binlog位置与列i_binlog_position相对应，gtid set与列
t_gtid_set相对应。

```
openGauss=> select * from sch_chameleon.t_replica_batch;
-[ RECORD 1 ]-----+------------------------------------------------
i_id_batch        | 1
i_id_source       | 1
t_binlog_name     | mysql-bin.000048
v_log_table       | t_log_replica_mysql_2
i_binlog_position | 3967
t_gtid_set        | a3ea3aee-ab76-11ed-9e33-fa163e3d2519:1-15198,
                  | c6eca988-a77e-11ec-8eec-fa163e3d2519:1-50459811
b_started         | f
b_processed       | f
b_replayed        | f
ts_created        | 2023-03-14 09:44:40.165798
ts_processed      |
ts_replayed       |
i_replayed        |
i_skipped         |
i_ddl             |
```

如上示例中，根据查询到的快照点若配置新增的snapshot相关的参数，需配置为如下：

```
snapshot.offset.binlog.filename=mysql-bin.000048
snapshot.offset.binlog.position=3967
snapshot.offset.gtid.set=a3ea3aee-ab76-11ed-9e33-fa163e3d2519:1-15197,c6eca988-a77e-11ec-8eec-fa163e3d2519:1-50459811
```

#### Sink端

(1) 启动类

```
connector.class=io.debezium.connector.mysql.sink.MysqlSinkConnector
```
(2) 配置文件示例

[mysql-sink.properties](https://gitee.com/opengauss/debezium/tree/master/debezium-connector-mysql/patch/mysql-sink.properties)

(3) 新增配置参数说明

| 参数                           | 类型   | 参数说明                                                                                                                                                                                                                                                                                                                                               |
|------------------------------| ------ |----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| topics                       | String | sink端从kafka抽取数据的topic                                                                                                                                                                                                                                                                                                                              |
| opengauss.driver             | String | openGauss驱动名                                                                                                                                                                                                                                                                                                                                       |
| opengauss.username           | String | openGauss用户名                                                                                                                                                                                                                                                                                                                                       |
| opengauss.password           | String | openGauss用户密码                                                                                                                                                                                                                                                                                                                                      |
| opengauss.url                | String | openGauss连接url                                                                                                                                                                                                                                                                                                                                     |
| parallel.replay.thread.num   | int    | 并行回放默认线程数量，默认为30                                                                                                                                                                                                                                                                                                                                   |
| xlog.location                | String | 增量迁移停止时openGauss端lsn的存储文件路径                                                                                                                                                                                                                                                                                                                        |
| schema.mappings              | String | mysql和openGauss的schema映射关系，与全量迁移chameleon配置相对应，用；区分不同的映射关系，用：区分mysql的database和openGauss的schema<br>例如chameleon的配置<br>schema_mappings:<br/>      mysql_database1: opengauss_schema1<br/>      mysql_database2: opengauss_schema2<br/>则sink端的schema.mappings参数需配置为schema.mappings=mysql_database1:opengauss_schema1;mysql_database2:opengauss_schema2 |
| commit.process.while.running | boolean | 是否开启迁移进度上报功能，默认为false，表示不开启该功能                                                                                                                                                                                                                                                                                                                     |
| sink.process.file.path       | String  | 迁移进度文件输出路径，默认在迁移插件同一目录下，在迁移进度上报功能开启后起作用                                                                                                                                                                                                                                                                                                            |
| commit.time.interval         | int     | 迁移进度上报的时间间隔，默认值为1，单位：秒，在迁移进度上报功能开启后起作用                                                                                                                                                                                                                                                                                                             |
| create.count.info.path       | String  | 源端binlog日志的事务号输出路径，默认在迁移插件同一目录下，必须与source端的该路径保持一致，用于和source端交互获取总体同步时延                                                                                                                                                                                                                                                                            |
| fail.sql.path                | String  | 回放失败的sql语句输出路径，默认在迁移插件同一目录下                                                                                                                                                                                                                                                                                                                        |
| process.file.count.limit| int     | 同一目录下文件数目限制，超过该数目工具会按时间从早到晚删除多余进度文件，默认为10                                                                                                                                                                                                                                                                                                          |
| process.file.time.limit | int     | 进度文件保存时间，超过该时间后工具会删除对应的进度文件，默认为168，单位：小时                                                                                                                                                                                                                                                                                                           |
| append.write | boolean | 进度文件写入方式，true表示追加写入，false表示覆盖写入，默认值为false                                                                                                                                                                                                                                                                                                          |
| file.size.limit | int     | 文件大小限制，超过该限制值工具会另启新文件写入，默认为10，单位：兆                                                                                                                                                                                                                                                                                                                 |
### 迁移进度上报信息说明

### source端

| 参数                           | 参数说明                                                                                                                                                                                                                                                                                                                                               |
|------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| timestamp                       | source端当前上报信息的时间戳                                                                                                                                                                                                                                                                                                                              |
| createCount             | 生产事务数（写入binlog的事务数）                                                                                                                                                                                                                                                                                                                                       |
| convertCount           | 完成解析的事务数
| pollCount                       | 存入kafka的事务数                                                                                                                                                                                                                                                                                                                              |
| rest             | source端剩余事务数（已生产但未存入kafka的事务数）                                                                                                                                                                                                                                                                                                                                       |
| speed           | source端处理速度（每秒处理的事务数）

### sink端

| 参数                           | 参数说明                                                                                                                                                                                                                                                                                                                                               |
|------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| timestamp                       | sink端当前上报信息的时间戳                                                                                                                                                                                                                                                                                                                              |
| extractCount             | 从kafka抽取的事务数                                                                                                                                                                                                                                                                                                                                       |
| skippedCount           | 跳过的事务数
| replayedCount                       | 已回放事务总数                                                                                                                                                                                                                                                                                                                              |
| successCount             | 回放成功的事务数                                                                                                                                                                                                                                                                                                                                       |
| failCount                       | 回放失败的事务数                                                                                                                                                                                                                                                                                                                              |
| skippedExcludeEventCount             | 跳过的黑名单表的事务数                                                                                                                                                                                                                                                                                                                                       |
| rest           | 剩余事务数（已抽取但未回放的事务数）
| speed           | sink端处理速度（每秒处理的事务数）
| overallPipe           | 当前时间片处于迁移管道中的事务总数

## 基于Debezium mysql connector进行在线迁移

### 环境依赖

kafka，zookeeper，confluent community，debezium-connector-mysql

### 原理

debezium mysql connector的source端，监控mysql数据库的binlog日志，并将数据（DDL和DML操作）以AVRO格式写入到kafka；debezium mysql connector的sink端，从kafka读取AVRO格式数据（DDL和DML操作），并组装为事务，在openGauss端按照事务粒度并行回放，从而完成数据（DDL和DML操作）从mysql在线迁移至openGauss端。

由于该方案严格保证事务的顺序性，因此将DDL和DML路由在kafka的一个topic下，且该topic的分区数只能为1(参数num.partitions=1)，从而保证source端推送到kafka，和sink端从kafka拉取数据都是严格保序的。

### 约束及限制

(1) MySQL5.7及以上版本；

(2) MySQL参数配置：

```
log_bin=on
binlog_format=row
binglog_row_image=full
gtid_mode=on #若未开启该参数，则sink端按照事务顺序串行回放，会降低在线迁移性能，且source端不会上报迁移进度
```
(3) 支持DML和DDL迁移，在线迁移直接透传DDL，对于openGauss和MySQL不兼容的语法，DDL迁移会报错；

(4) Kafka中以AVRO格式存储数据，AVRO字段名称[命名规则](https://avro.apache.org/docs/1.11.1/specification/#names)为：
```
- 以[A-Za-z_]开头
- 随后仅包含[A-Za-z0-9_]
```
因此，对于MySQL中的标识符命名，包括表名、列名等，需满足上述命名规范，否则在线迁移会报错。

### 性能指标

利用sysbench进行测试，在openEuler arm操作系统2p Kunpeng-920机器，针对混合IUD场景，10张表50个线程（insert-30线程，update-10线程，delete-10线程），性能可达3w tps。

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

- [debezium-connector-mysql](https://opengauss.obs.cn-south-1.myhuaweicloud.com/latest/tools/replicate-mysql2openGauss-5.0.0.tar.gz)

  ```
  wget -c https://opengauss.obs.cn-south-1.myhuaweicloud.com/latest/tools/replicate-mysql2openGauss-5.0.0.tar.gz
  
  tar -zxvf replicate-mysql2openGauss-5.0.0.tar.gz
  ```

#### 修改配置文件

默认配置文件的地址均为localhost，若需修改为具体ip，请同步修改下列所有文件中参数涉及localhost的均改为实际ip。

- zookeeper

  ```
  配置文件位置：/kafka_2.13-3.2.3/config/zookeeper.properties
  ```
  zookeeper的默认端口号为2181，对应参数clientPort=2181。
  
  若端口冲突，需要修改端口号，则同步修改以下文件对应参数：
  ```
  kafka_2.13-3.2.3/config/zookeeper.properties------clientPort=2181
  kafka_2.13-3.2.3/config/server.properties------zookeeper.connect=localhost:2181
  confluent-5.5.1/etc/schema-registry/schema-registry.properties------kafkastore.connection.url=localhost:2181
   ```

- kafka

  ```
  配置文件位置：/kafka_2.13-3.2.3/config/server.properties
  ```

  注意topic的分区数必须为1，因此需设置参数num.partitions=1，该参数默认值即为1，因此无需单独修改该参数。

  kafka的默认端口是9092，对应参数listeners=PLAINTEXT://:9092。

  若端口冲突，需要修改端口号，则同步修改以下文件对应参数：
  ```
  kafka_2.13-3.2.3/config/server.properties------listeners=PLAINTEXT://:9092
  confluent-5.5.1/etc/schema-registry/schema-registry.properties------kafkastore.bootstrap.servers=PLAINTEXT://localhost:9092
  confluent-5.5.1/etc/schema-registry/connect-avro-standalone.properties------bootstrap.servers=localhost:9092
  confluent-5.5.1/etc/kafka/mysql-source.properties------database.history.kafka.bootstrap.servers=127.0.0.1:9092
  ```

- schema-registry

  ```
  配置文件位置：/confluent-5.5.1/etc/schema-registry/schema-registry.properties
  ```
  schema-registry的默认端口是8081，对应参数listeners=http://0.0.0.0:8081。

  若端口冲突，需要修改端口号，则同步修改以下文件对应参数：
  ```
  confluent-5.5.1/etc/schema-registry/schema-registry.properties------listeners=http://0.0.0.0:8081
  confluent-5.5.1/etc/schema-registry/connect-avro-standalone.properties------key.converter.schema.registry.url=http://localhost:8081
  confluent-5.5.1/etc/schema-registry/connect-avro-standalone.properties------value.converter.schema.registry.url=http://localhost:8081
  ```

- connect-standalone

  ```
  配置文件位置：/confluent-5.5.1/etc/schema-registry/connect-avro-standalone.properties
  ```

  注意在plugin.path配置项中增加debezium-connector-mysql所在的路径

  若debezium-connector-mysql所在路径为：/data/debezium_kafka/plugin/debezium-connector-mysql

  则配置其上一层目录，即plugin.path=share/java,/data/debezium_kafka/plugin

  connect-standalone的默认端口是8083，对应参数rest.port=8083。

  若端口冲突，需要修改端口号，则同步修改以下文件对应参数：
  ```
  confluent-5.5.1/etc/schema-registry/connect-avro-standalone.properties------rest.port=8083
  ```

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

步骤(4)和(5)示例source端和sink端分开启动，推荐分开启动方式。两者也可以同时启动，同时启动的命令为：

```
cd confluent-5.5.1
./bin/connect-standalone etc/schema-registry/connect-avro-standalone.properties etc/kafka/mysql-source.properties etc/kafka/mysql-sink.properties
```

其他命令：

（1）查看topic

```
cd kafka_2.13-3.2.3
./bin/kafka-topics.sh --bootstrap-server 127.0.0.1:9092 --list
```

（2）查看topic的信息

```
cd kafka_2.13-3.2.3
./bin/kafka-topics.sh --bootstrap-server 127.0.0.1:9092 --describe --topic topic_name
```

（3）查看topic的内容

```
cd confluent-5.5.1
./bin/kafka-avro-console-consumer --bootstrap-server 127.0.0.1:9092 --topic topic_name --from-beginning
```

### 全量与增量迁移的配合

(1) 启动全量迁移

全量迁移使用[chameleon](https://gitee.com/opengauss/openGauss-tools-chameleon)完成，可实现表，数据，函数，存储过程，视图，触发器的
离线迁移，使用指南请参考[使用指南](https://gitee.com/opengauss/openGauss-tools-chameleon/blob/master/chameleon%E4%BD%BF%E7%94%A8%E6%8C%87%E5%8D%97.md)

全量迁移启动后，可在openGauss端的表sch_chameleon.t_replica_batch中查询到全量迁移的快照点，单个表的快照点存储在
sch_chameleon.t_replica_tables中。

(2) 启动source端

启动source端开始前，需首先启动zookeeper，kafka，并注册schema。

查询到全量迁移的快照点后，即可在source端的配置文件mysql-source.properties中配置全量迁移的快照点，并启动source端，无需等待全量迁移结束后
才可启动source端。全量迁移启动后，即可启动source端，这样可以尽可能减少source端的时延，以达到减少迁移延迟的目的。

当然，也可以等待全量迁移结束后启动source端。

启动source端后，针对全量迁移的表，若对其的DML事务位于表的快照点之前，将跳过对应的DML操作，避免数据出现重复，可保证迁移过程中数据不丢失，不重复。

(3) 全量迁移结束，启动sink端

等待全量迁移结束后，即可启动sink端回放数据。

若在全量迁移未结束时，就启动sink端，将会导致数据乱序，属于不合理的操作步骤，实际操作过程应避免不合理的操作。

### 性能测试

#### 性能指标

利用sysbench进行测试，在openEuler arm操作系统2p Kunpeng-920机器，针对混合IUD场景，10张表50个线程（insert-30线程，update-10线程，delete-10线程），性能可达3w tps。

#### 配置条件
(1) mysql

- mysql参数配置：
  ```
  log_bin=on
  binlog_format=row
  binglog_row_image=full
  gtid_mode=on
  ```
- binlog位置、安装目录、数据目录分别部署在3个不同的NVME盘
  
- mysql高性能配置

(2) openGauss

- pg_xlog、安装目录、数据目录分别部署在3个不同的NVME盘
  
- openGauss高性能配置

(3) 在线迁移工具Debezium mysql connector

- source端参数配置：
  ```
  parallel.parse.event=true
  ```
- 修改Java heap space参数，否则可能出现OutOfMemory问题。
  
  kafka connect进程启动时默认参数为-Xms256M -Xmx2G，将该参数值调大，
  修改为-Xms25G -Xmx25G，修改的文件位置为：
  ```
  confluent-5.5.1/bin/connect-standalone 58行 export KAFKA_HEAP_OPTS="-Xms25G -Xmx25G"
  ```
- 在java11环境上运行在线迁移工具

- 设置kafka-connect日志级别为WARN

  默认的日志级别为INFO，INFO级别的日志会输出回放的事务信息，为减少日志刷屏，
  建议将日志级别修改为WARN，此时只显示迁移效率日志。
  
  修改的文件位置为：
  ```
  confluent-5.5.1/etc/kafka/connect-log4j.properties 16行
  log4j.rootLogger=WARN, stdout, connectAppender
  ```

#### 测试步骤

(1) sysbench执行prepare命令，为mysql端准备数据

(2) 通过chameleon离线迁移数据至openGauss端

(3) 开启在线复制

启动zookeeper，kafka，注册schema

绑核启动source端
```
cd confluent-5.5.1
numactl -C 32-63 -m 0 ./bin/connect-standalone etc/schema-registry/connect-avro-standalone.properties etc/kafka/mysql-source.properties
```

绑核启动sink端
```
cd confluent-5.5.1
numactl -C 64-95 -m 0 ./bin/connect-standalone etc/schema-registry/connect-avro-standalone-1.properties etc/kafka/mysql-sink.properties
```
(4) sysbench执行run命令，给mysql压入数据

混合IUD场景，10张表50个线程（insert-30线程，update-10线程，delete-10线程）

(5) 统计迁移工具日志，得到迁移效率

### FAQ

(1) schema-registry报错: Schema being registered is incompatible with an earlier schema

解决方案：
停止schema-registry进程，执行下面curl命令，并重新启动schema-registry和kafka-connect

可根据实际配置修改ip:localhost和端口:8081
```
curl -X GET http://localhost:8081/config

curl -X PUT -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data '{"compatibility": "NONE"}' \
  http://localhost:8081/config
```

## Debezium opengauss connector

### 功能介绍

新增的debezium opengauss connector作为source端，可用于捕获数据变更并存入kafka。在此基础上添加sink端功能，功能点如下：

- 支持openGauss端对schema下的数据的dml操作同步到MySQL端，不支持迁移ddl操作的迁移；
- Sink端支持数据按表进行并发回放；
- 支持openGausss的多个schema下的数据迁移到指定的MySQL的多个库；
- 增加迁移进度上报功能，可用于读取数据迁移时延。

### 新增配置参数说明

#### source端

```
connector.class=io.debezium.connector.opengauss.OpengaussConnector
```

| 参数                           | 类型     | 参数说明                                                                |
|------------------------------|--------|---------------------------------------------------------------------|
| xlog.location                | String | 自定义配置xlog的位置                                                        |
| wal.sender.timeout           | int    | 自定义数据库等待迁移工具接收日志的最大等待时间，对于openGauss 3.0.x版本，此值默认为12000,单位：毫秒        |
| commit.process.while.running | boolean | 是否开启迁移进度上报功能，默认为false，表示不开启该功能                                      |
| source.process.file.path     | String  | 迁移进度文件输出路径，默认在迁移插件同一目录下，在迁移进度上报功能开启后起作用                             |
| commit.time.interval         | int     | 迁移进度上报的时间间隔，默认值为1，单位：秒，在迁移进度上报功能开启后起作用                              |
| create.count.info.path       | String  | 源端xlog日志写入的dml操作总数，默认在迁移插件同一目录下，必须与sink端的该路径保持一致，用于和sink端交互获取总体同步时延 |
| process.file.count.limit| int     | 同一目录下文件数目限制，超过该数目工具会按时间从早到晚删除多余进度文件，默认为10                           |
| process.file.time.limit | int     | 进度文件保存时间，超过该时间后工具会删除对应的进度文件，默认为168，单位：小时                            |
| append.write | boolean | 进度文件写入方式，true表示追加写入，false表示覆盖写入，默认值为false                           |
| file.size.limit | int     | 文件大小限制，超过该限制值工具会另启新文件写入，默认为10，单位：兆                                  |

xlog参数详细说明：

- 使用前提

（1）用户已在源端数据库建立逻辑复制槽和发布

```
SELECT * FROM pg_create_logical_replication_slot('slot_name', 'pgoutput');
CREATE PUBLICATION dbz_publication FOR ALL TABLES;
```

（2）删除kafka临时文件里的connect.offsets

- 使用方法

（1）查询当前xlog位置

```
select * from pg_current_xlog_location();
```

（2）将查询到的xlog位置配到迁移工具source端的配置文件里

- 说明

若source端无此配置项，工具将从kafka中记录的位置开始迁移，若kafka中没有记录，工具默认从建立逻辑复制槽和发布之后的位置开始迁移

#### Sink端

```
connector.class=io.debezium.connector.opengauss.sink.OpengaussSinkConnector
```

| 参数                           | 类型   | 参数说明                                                                                                                                                                                                                                                                                                                                               |
|------------------------------| ------ |----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| topics                       | String | sink端从kafka抽取数据的topic                                                                                                                                                                                                                                                                                                                              |
| max.thread.count             | String | 自定义Sink端按表回放时的最大并发线程数（不能为0）                                                                                                                                                                                                                                                                                                                        |
| mysql.username               | String | MySQL用户名                                                                                                                                                                                                                                                                                                                                           |
| mysql.password               | String | MySQL用户密码                                                                                                                                                                                                                                                                                                                                          |
| mysql.url                    | String | MySQL连接url                                                                                                                                                                                                                                                                                                                                         |
| schema.mappings              | String | openGauss的schema与MySQL的映射关系，与全量迁移chameleon配置相反，用；区分不同的映射关系，用：区分openGauss的schema与MySQL的database<br>例如chameleon的配置<br>schema_mappings:<br/>      mysql_database1: opengauss_schema1<br/>      mysql_database2: opengauss_schema2<br/>则sink端的schema.mappings参数需配置为schema.mappings=opengauss_schema1:mysql_database1;opengauss_schema2:mysql_database2 |
| commit.process.while.running | boolean | 是否开启迁移进度上报功能，默认为false，表示不开启该功能                                                                                                                                                                                                                                                                                                                     |
| sink.process.file.path       | String  | 迁移进度文件输出路径，默认在迁移插件同一目录下，在迁移进度上报功能开启后起作用                                                                                                                                                                                                                                                                                                            |
| commit.time.interval         | int     | 迁移进度上报的时间间隔，默认值为1，单位：秒，在迁移进度上报功能开启后起作用                                                                                                                                                                                                                                                                                                             |
| create.count.info.path       | String  | 源端xlog日志写入的dml操作总数，默认在迁移插件同一目录下，必须与source端的该路径保持一致，用于和source端交互获取总体同步时延                                                                                                                                                                                                                                                                            |
| fail.sql.path                | String  | 回放失败的sql语句输出路径，默认在迁移插件同一目录下                                                                                                                                                                                                                                                                                                                        |
| process.file.count.limit     | int     | 同一目录下文件数目限制，超过该数目工具会按时间从早到晚删除多余进度文件，默认为10                                                                                                                                                                                                                                                                                                          |
| process.file.time.limit      | int     | 进度文件保存时间，超过该时间后工具会删除对应的进度文件，默认为168，单位：小时                                                                                                                                                                                                                                                                                                           |
| append.write                 | boolean | 进度文件写入方式，true表示追加写入，false表示覆盖写入，默认值为false                                                                                                                                                                                                                                                                                                          |
| file.size.limit              | int     | 文件大小限制，超过该限制值工具会另启新文件写入，默认为10，单位：兆                                                                                                                                                                                                                                                                                                                 |

### 迁移进度上报信息说明

### source端

| 参数                           | 参数说明                                                                                                                                                                                                                                                                                                                                               |
|------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| timestamp                       | source端当前上报信息的时间戳                                                                                                                                                                                                                                                                                                                              |
| createCount             | 生产数据量（写入xlog的数据量）                                                                                                                                                                                                                                                                                                                                       |
| convertCount           | 完成解析数据量
| pollCount                       | 存入kafka的数据量                                                                                                                                                                                                                                                                                                                              |
| skippedExcludeDataCount                       | 跳过的黑名单表的数据量                                                                                                                                                                                                                                                                                                                              |
| rest             | 剩余数据量（已写入xlog但未存入kafka的数据量）                                                                                                                                                                                                                                                                                                                                       |
| speed           | source端处理速度（每秒处理的数据量）

### sink端

| 参数                           | 参数说明                                                                                                                                                                                                                                                                                                                                               |
|------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| timestamp                       | sink端当前上报信息的时间戳                                                                                                                                                                                                                                                                                                                              |
| extractCount             | 从kafka抽取的数据量                                                                                                                                                                                                                                                                                                                                       |
| replayedCount                       | 已完成回放的数据量                                                                                                                                                                                                                                                                                                                              |
| successCount             | 回放成功的数据量                                                                                                                                                                                                                                                                                                                                       |
| failCount                       | 回放失败的数据量                                                                                                                                                                                                                                                                                                                              |
| rest           | 剩余数据量（已抽取但未回放）
| speed           | sink端处理速度（每秒处理的数据量）
| overallPipe           | 当前时间片处于迁移管道中的数据总数

## 基于Debezium opengauss connector进行反向迁移

### 环境依赖

kafka， zookeeper，confluent community，debezium-connector-opengauss

### 原理

debezium opengauss connector的source端，监控openGauss数据库的逻辑日志，并将数据写入到kafka；debezium opengauss connector的sink端，从kafka读取数据，并组装为sql语句，在MySQL端按表并行回放，从而完成数据从openGauss在线迁移至MySQL端。

### 前置条件

openGauss开启逻辑复制功能：

    （1）仅限初始用户和拥有REPLICATION权限的用户进行操作。三权分立关闭时数据库管理员可以进行逻辑复制操作，三权分立开启时不允许数据库管理员进行逻辑复制操作；
    （2）openGauss的库与逻辑复制槽一一对应，当待迁移的库改变时，需要配置新的逻辑复制槽的名字；
    （3）初次使用时要以sysadmin权限的用户开启工具。

openGauss参数配置：

```
ssl=on
wal_level=logical
```
约束：

    （1）反向迁移sink端按表分发数据，不支持按事务分发，日志中记录的回放条数为实际成功执行的sql语句条数，openGauss分区表执行update操作时，如果更新前的数据和更新后的数据在同一分区，只会执行一条update语句，如果不在同一分区，会以事务的形式先后执行一条delete语句和一条insert语句，这种情形下日志会显示回放了两条数据；
    （2）反向迁移connector端配置连接数据库的用户需要有对应数据库下所有schema以及所有表的操作权限
    （3）反向迁移数据类型映射与变色龙的默认数据类型映射相反，当两端数据类型不一致时，只能迁移两端数据类型都支持的数据变更

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

- [debezium-connector-opengauss](https://opengauss.obs.cn-south-1.myhuaweicloud.com/latest/tools/replicate-openGauss2mysql-5.0.0.tar.gz)

  ```
  wget -c https://opengauss.obs.cn-south-1.myhuaweicloud.com/latest/tools/replicate-openGauss2mysql-5.0.0.tar.gz
  
  tar -zxvf replicate-openGauss2mysql-5.0.0.tar.gz
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

利用sysbench进行测试，在openEuler arm操作系统2p Kunpeng-920机器，针对混合IUD场景，50张表50个线程（insert-30线程，update-10线程，delete-10线程），性能可达1w tps。

### FAQ

(1) schema-registry报错: Schema being registered is incompatible with an earlier schema

解决方案：
停止schema-registry进程，执行下面curl命令，并重新启动schema-registry和kafka-connect

可根据实际配置修改ip:localhost和端口:8081
```
curl -X GET http://localhost:8081/config

curl -X PUT -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data '{"compatibility": "NONE"}' \
  http://localhost:8081/config
```