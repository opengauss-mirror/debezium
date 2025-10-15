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

## 迁移插件下载
当前Debezium mysql connector和Debezium opengauss connector作为openGauss数据迁移平台的组件，可在[官网下载页面](https://opengauss.org/zh/download/)的openGauss Tools部分下载各版本的发布包。
- replicate-mysql2openGauss
基于Debezium mysql connector构建，支持对MySQL增量数据的同步

- replicate-openGauss2mysql
基于Debezium opengauss connector构建，支持对openGauss全量数据和增量数据的同步

获取软件包后，需对其完整性进行校验，操作步骤如下：

1. 计算下载包的sha256值（以replicate-mysql2openGauss_6.0.0为例，其他版本操作相同）
~~~
sha256sum replicate-mysql2openGauss-6.0.0.tar.gz
~~~

2. 在[官网下载页面](https://opengauss.org/zh/download/)的openGauss Tools部分中复制对应软件包的sha256值，与步骤1计算出的sha256值做对比，如果一致则可以确认下载下来的包是完整的，否则需要重新下载。

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

我们基于原始的debezium开源软件的debezium postgresql connector，新增了debezium opengauss connector，支持抽取openGauss的逻辑日志，将其存入kafka的topic中。同时，我们增加了debezium opengauss connector的sink端能力，能够抽取kafka的日志并完成数据的回放，以实现对数据dml操作的反向迁移能力（openGauss -> mysql, openGauss -> PostgreSQL）.
编译debezium后，可以得到反向迁移工具的压缩包，压缩包的位置为：

```
debezium-connector-openngauss/target/debezium-connector-opengauss-1.8.1.Final-plugin.tar.gz
```

### Debezium postgres connector

基于原始的debezium开源软件的debezium postgresql connector，支持抽取postgresql的逻辑日志，将其存入kafka的topic中。同时，我们增加了debezium postgres connector 全量迁移和sink端能力，全量迁移支持抽取postgresql端全量数据和对象，并将对应消息存入kafka中。增量迁移通过抽取postgresql的逻辑日志并存入kafka中。sink端能够抽取kafka的日志并完成数据的回放，以实现对数据的全量和增量迁移能力（PostgreSQL -> openGauss）.
编译debezium后，可以得到PostgreSQL迁移工具的压缩包，压缩包的位置为：

```
debezium-connector-postgres/target/debezium-connector-postgres-1.8.1.Final-plugin.tar.gz
```

### 构建命令

```
mvn clean package -P quick,skip-integration-tests,oracle,jdk11,assembly,xstream,xstream-dependency,skip-tests -Dgpg.skip -Dmaven.test.skip=true -Denforcer.skip=true
```

## Debezium mysql connector

### 新增功能介绍

原始的debezium mysql connector作为source端，可用于捕获数据变更并存入kafka。现新增如下功能点：

- Source端支持并行解析event事件；
- Source端支持自定义配置快照点；
- 配置gtid_mode=on，source端支持解析last_committed和sequence_number字段，并存入kafka；
- 基于Debezium connector（Kafka Connect）框架，增加sink端能力，可用于从kafka抽取数据并在openGauss端按照事务粒度并行回放;
- 增加迁移进度上报功能，可用于读取数据迁移时延；
- 增加增量迁移断点续传功能，用户中断后基于断点重启后继续迁移；
- sink端增加按表并行回放的能力，并支持自定义参数控制按事务回放还是按表回放。

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

| 参数                             | 类型      | 参数说明                                                                                                                                                          |
|--------------------------------|---------|---------------------------------------------------------------------------------------------------------------------------------------------------------------|
| snapshot.offset.binlog.filename | String  | 自定义配置快照点的binlog文件名                                                                                                                                            |
| snapshot.offset.binlog.position | String  | 自定义配置快照点的binlog位置                                                                                                                                             |
| snapshot.offset.gtid.set       | String  | 自定义配置快照点的Executed_Gtid_Set，需注意最大事务号需减1                                                                                                                        |
| parallel.parse.event           | boolean | 是否启用并行解析event能力，默认为true，表示启用并行解析能力                                                                                                                            |
| commit.process.while.running   | boolean | 是否开启迁移进度上报功能，默认为false，表示不开启该功能                                                                                                                                |
| source.process.file.path       | String  | 迁移进度文件输出路径，默认在迁移插件同一目录下，在迁移进度上报功能开启后起作用                                                                                                                       |
| commit.time.interval           | int     | 迁移进度上报的时间间隔，默认值为1，单位：秒，在迁移进度上报功能开启后起作用                                                                                                                        |
| create.count.info.path         | String  | 源端binlog日志的事务号输出路径，默认在迁移插件同一目录下，必须与sink端的该路径保持一致，用于和sink端交互获取总体同步时延                                                                                           |
| process.file.count.limit       | int     | 同一目录下文件数目限制，超过该数目工具会按时间从早到晚删除多余进度文件，默认为10                                                                                                                     |
| process.file.time.limit        | int     | 进度文件保存时间，超过该时间后工具会删除对应的进度文件，默认为168，单位：小时                                                                                                                      |
| append.write                   | boolean | 进度文件写入方式，true表示追加写入，false表示覆盖写入，默认值为false                                                                                                                     |
| file.size.limit                | int     | 文件大小限制，超过该限制值工具会另启新文件写入，默认为10，单位：兆                                                                                                                            |
| min.start.memory               | String  | 自定义配置debezium最小启动内存，通过脚本生效，默认为256M                                                                                                                            |
| max.start.memory               | String  | 自定义配置debezium最大启动内存，通过脚本生效，默认为2G                                                                                                                              |
| queue.size.limit               | int     | source端抽取binlog事件存储队列的最大长度，int类型，默认值为1000000                                                                                                                  |
| open.flow.control.threshold    | double  | 流量控制参数，double类型，默认值为0.8，当存储binlog事件的某一队列长度>最大长度queue.size.limit*该门限值时，将启用流量控制，暂停抽取binlog事件                                                                    |
| close.flow.control.threshold   | double  | 流量控制参数，double类型，默认值为0.7，当存储binlog事件的各个队列长度<最大长度queue.size.limit*该门限值时，将关闭流量控制，继续抽取binlog事件                                                                    |
| kafka.bootstrap.server         | String  | 自定义记录source端与sink端关联参数的Kafka启动服务器地址，可根据实际情况修改，默认值为localhost:9092                                                                                              |
| provide.transaction.metadata         | boolean | debezium原生参数，指定连接器是否用事务形式封装binlog事件，当配置为true时，增量数据以事务传递，回放方式为按事务并行回放，当配置为false时，增量数据以单条更新事件传递，回放方式为按表并行回放，默认值为false；注意：该配置项每次修改过后首次启动时，须等source端启动成功后再启动sink端 |
| wait.timeout.second          | int     | 自定义JDBC连接在被服务器自动关闭之前等待活动的秒数。如果客户端在这段时间内没有向服务器发送任何请求，服务器将关闭该连接，默认值：28800，单位：秒



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

| 参数                                        | 类型      | 参数说明                                                                                                                                                                                                                                                                                                                                               |
|-------------------------------------------|---------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| topics                                    | String  | sink端从kafka抽取数据的topic                                                                                                                                                                                                                                                                                                                              |
| opengauss.driver                          | String  | openGauss驱动名                                                                                                                                                                                                                                                                                                                                       |
| opengauss.username                        | String  | openGauss用户名                                                                                                                                                                                                                                                                                                                                       |
| opengauss.password                        | String  | openGauss用户密码                                                                                                                                                                                                                                                                                                                                      |
| opengauss.url                             | String  | openGauss连接url                                                                                                                                                                                                                                                                                                                                     |
| parallel.replay.thread.num                | int     | 并行回放默认线程数量，默认为30                                                                                                                                                                                                                                                                                                                                   |
| xlog.location                             | String  | 增量迁移停止时openGauss端lsn的存储文件路径                                                                                                                                                                                                                                                                                                                        |
| schema.mappings                           | String  | mysql和openGauss的schema映射关系，与全量迁移chameleon配置相对应，用；区分不同的映射关系，用：区分mysql的database和openGauss的schema<br>例如chameleon的配置<br>schema_mappings:<br/>      mysql_database1: opengauss_schema1<br/>      mysql_database2: opengauss_schema2<br/>则sink端的schema.mappings参数需配置为schema.mappings=mysql_database1:opengauss_schema1;mysql_database2:opengauss_schema2 |
| commit.process.while.running              | boolean | 是否开启迁移进度上报功能，默认为false，表示不开启该功能                                                                                                                                                                                                                                                                                                                     |
| sink.process.file.path                    | String  | 迁移进度文件输出路径，默认在迁移插件同一目录下，在迁移进度上报功能开启后起作用                                                                                                                                                                                                                                                                                                            |
| commit.time.interval                      | int     | 迁移进度上报的时间间隔，默认值为1，单位：秒，在迁移进度上报功能开启后起作用                                                                                                                                                                                                                                                                                                             |
| create.count.info.path                    | String  | 源端binlog日志的事务号输出路径，默认在迁移插件同一目录下，必须与source端的该路径保持一致，用于和source端交互获取总体同步时延                                                                                                                                                                                                                                                                            |
| fail.sql.path                             | String  | 回放失败的sql语句输出路径，默认在迁移插件同一目录下                                                                                                                                                                                                                                                                                                                        |
| process.file.count.limit                  | int     | 同一目录下文件数目限制，超过该数目工具会按时间从早到晚删除多余进度文件，默认为10                                                                                                                                                                                                                                                                                                          |
| process.file.time.limit                   | int     | 进度文件保存时间，超过该时间后工具会删除对应的进度文件，默认为168，单位：小时                                                                                                                                                                                                                                                                                                           |
| append.write                              | boolean | 进度文件写入方式，true表示追加写入，false表示覆盖写入，默认值为false                                                                                                                                                                                                                                                                                                          |
| file.size.limit                           | int     | 文件大小限制，超过该限制值工具会另启新文件写入，默认为10，单位：兆                                                                                                                                                                                                                                                                                                                 |
| queue.size.limit                          | int     | 存储kafka记录的队列的最大长度，int类型，默认值为1000000                                                                                                                                                                                                                                                                                                                |
| open.flow.control.threshold               | double  | 流量控制参数，double类型，默认值为0.8，当存储kafka记录的队列长度>最大长度queue.size.limit*该门限值时，将启用流量控制，暂停从kafka抽取数据                                                                                                                                                                                                                                                            |
| close.flow.control.threshold              | double  | 流量控制参数，double类型，默认值为0.7，当存储kafka记录的队列长度<最大长度queue.size.limit*该门限值时，将关闭流量控制，继续从kafka抽取数据                                                                                                                                                                                                                                                            |
| record.breakpoint.kafka.topic             | String  | 自定义断点记录topic，在回放过程记录执行结果到Kafka中，可根据实际情况修改，默认值为bp_topic                                                                                                                                                                                                                                                                                             |
| record.breakpoint.kafka.bootstrap.servers | String  | 自定义断点记录的Kafka启动服务器地址，如无特殊需要，配置为source端的Kafka地址，可根据实际情况修改，默认值为localhost:9092                                                                                                                                                                                                                                                                        |
| record.breakpoint.kafka.attempts          | int     | 自定义读取断点记录重试次数，默认为3                                                                                                                                                                                                                                                                                                                                 |
| record.breakpoint.kafka.size.limit        | int     | 断点记录Kafka的条数限制，超过该限制会触发删除Kafka的断点清除策略，删除无用的断点记录数据，单位：事务万条数，默认值3000                                                                                                                                                                                                                                                                                 |
| record.breakpoint.kafka.clear.interval    | int     | 断点记录Kafka的时间限制，超过该限制会触发删除Kafka的断点清除策略，删除无用的断点记录数据，单位：小时，默认值1                                                                                                                                                                                                                                                                                       |
| record.breakpoint.repeat.count.limit      | int     | 断点续传时，查询待回放数据是否已在断点之前备回放的数据条数，默认值：50000 
| wait.timeout.second                       | long    | sink端数据库停止服务后迁移工具等待数据库恢复服务的最大时长，默认值：28800，单位：秒
| database.standby.hostnames                | String  | sink端数据库是主备部署时的备机ip列表，用逗号隔开，需与port列表一一对应，此配置项只对sink端是openGauss时起作用，默认值：""，不配置此项时sink端只连接主节点
| database.standby.ports                    | String  | sink端数据库是主备部署时的备机port列表，用逗号隔开，需与ip列表一一对应，此配置项只对sink端是openGauss时起作用，默认值：""，不配置此项时sink端只连接主节点

### 迁移进度上报信息说明

#### Source端

| 参数                           | 参数说明                           |
|------------------------------|--------------------------------|
| timestamp                       | source端当前上报信息的时间戳              |
| createCount             | 生产事务数（写入binlog的事务数）            |
| skippedExcludeCount             | source端跳过的黑名单之内或白名单之外的变更数      |
| convertCount | 完成解析的事务数 |
| pollCount | 存入kafka的事务数 |
| rest | source端剩余事务数（已生产但未存入kafka的事务数） |
| speed | source端处理速度（每秒处理的事务数） |
#### Sink端

| 参数                     | 参数说明                             |
| ------------------------ | ------------------------------------ |
| timestamp                | sink端当前上报信息的时间戳           |
| extractCount             | 从kafka抽取的事务数                  |
| skippedCount             | 跳过的事务数                         |
| replayedCount            | 已回放事务总数                       |
| successCount             | 回放成功的事务数                     |
| failCount                | 回放失败的事务数                     |
| skippedExcludeEventCount | 跳过的黑名单表的事务数               |
| rest                     | 剩余事务数（已抽取但未回放的事务数） |
| speed                    | sink端处理速度（每秒处理的事务数）   |
| overallPipe              | 当前时间片处于迁移管道中的事务总数   |

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
因此，对于MySQL中的标识符，表名和列名推荐按上述规范命名，否则在线迁移可能会报错。

(5) 增量迁移过程中创建的表也会进行迁移，在此过程中创建的表的字符集应与opengauss的server端字符集一致，否则会迁移失败。

### 性能指标

按事务回放时，利用sysbench进行测试，在openEuler arm操作系统2p Kunpeng-920机器，针对混合IUD场景，10张表50个线程（insert-30线程，update-10线程，delete-10线程），性能可达3w tps；
按表回放时，利用sysbench进行测试，在openEuler arm操作系统2p Kunpeng-920机器，针对混合IUD场景，50张表50个线程（insert-30线程，update-10线程，delete-10线程），性能可达3w tps。

### 部署过程

#### 下载依赖

- [kafka](https://mirrors.tuna.tsinghua.edu.cn/apache/kafka/)
（以kafka_2.13-3.6.1为例）
  ```
  wget -c https://mirrors.tuna.tsinghua.edu.cn/apache/kafka/3.6.1/kafka_2.13-3.6.1.tgz  
  
  tar -zxf kafka_2.13-3.6.1.tgz 
  ```

- [confluent community](https://packages.confluent.io/archive/5.5/confluent-community-5.5.1-2.12.zip)

  ```
  wget -c  https://packages.confluent.io/archive/5.5/confluent-community-5.5.1-2.12.zip
  
  unzip confluent-community-5.5.1-2.12.zip
  ```

- [debezium-connector-mysql](https://opengauss.obs.cn-south-1.myhuaweicloud.com/latest/tools/replicate-mysql2openGauss-7.0.0rc3.tar.gz)

  ```
  wget -c https://opengauss.obs.cn-south-1.myhuaweicloud.com/latest/tools/replicate-mysql2openGauss-7.0.0rc3.tar.gz
  
  tar -zxvf replicate-mysql2openGauss-7.0.0rc3.tar.gz
  ```

#### 修改配置文件

默认配置文件的地址均为localhost，若需修改为具体ip，请同步修改下列所有文件中参数涉及localhost的均改为实际ip。

- zookeeper

  ```
  配置文件位置：/kafka_2.13-3.6.1/config/zookeeper.properties
  ```
  zookeeper的默认端口号为2181，对应参数clientPort=2181。
  
  若端口冲突，需要修改端口号，则同步修改以下文件对应参数：
  ```
  kafka_2.13-3.6.1/config/zookeeper.properties------clientPort=2181
  kafka_2.13-3.6.1/config/server.properties------zookeeper.connect=localhost:2181
  confluent-5.5.1/etc/schema-registry/schema-registry.properties------kafkastore.connection.url=localhost:2181
  ```

- kafka

  ```
  配置文件位置：/kafka_2.13-3.6.1/config/server.properties
  ```

  注意topic的分区数必须为1，因此需设置参数num.partitions=1，该参数默认值即为1，因此无需单独修改该参数。

  kafka的默认端口是9092，对应参数listeners=PLAINTEXT://:9092。

  若端口冲突，需要修改端口号，则同步修改以下文件对应参数：
  ```
  kafka_2.13-3.6.1/config/server.properties------listeners=PLAINTEXT://:9092
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
  若需查看kafka topic内容，需修改
  confluent-5.5.1/bin/kafka-avro-console-consumer------DEFAULT_SCHEMA_REGISTRY_URL="--property schema.registry.url=http://192.168.0.219:8081"
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
cd kafka_2.13-3.6.1
./bin/zookeeper-server-start.sh ./config/zookeeper.properties
```

（2）启动kafka

```
cd kafka_2.13-3.6.1
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
cd kafka_2.13-3.6.1
./bin/kafka-topics.sh --bootstrap-server 127.0.0.1:9092 --list
```

（2）查看topic的信息

```
cd kafka_2.13-3.6.1
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

按事务并发时，混合IUD场景，10张表50个线程（insert-30线程，update-10线程，delete-10线程）
按表并发时，混合IUD场景，50张表50个线程（insert-30线程，update-10线程，delete-10线程），性能可达3w tps。

(5) 统计迁移工具日志，得到迁移效率



## Debezium postgres connector

### 新增功能介绍

原始的debezium postgres connector作为source端，可用于捕获数据变更并存入kafka。现新增如下功能点：

- 支持全量数据迁移和对象迁移；

- Source端支持自定义配置快照点；
- 基于Debezium connector（Kafka Connect）框架，增加sink端能力，可用于从kafka抽取数据并在openGauss端根据迁移类型进行数据回放;
- 增加增量迁移进度上报功能，可用于读取数据迁移时延；

### 新增配置参数说明

#### Source端

(1) 启动类

```
connector.class=io.debezium.connector.postgresql.PostgresConnector
```

(2) 配置文件示例

[postgres-source.properties](https://gitee.com/opengauss/debezium/blob/master/debezium-connector-postgres/patch/postgres-source.properties)

(3) debezium原生参数含义请参考：

[debezium原生参数](https://debezium.io/documentation/reference/1.8/connectors/postgresql.html)

(4) topic路由请参考：

[topic路由](https://debezium.io/documentation/reference/stable/transformations/topic-routing.html)

在线迁移方案严格保证事务的顺序性，因此将DML路由在kafka的一个topic下，且该topic的分区数只能为1(参数num.partitions=1)，从而保证source端推送到kafka，和sink端从kafka拉取数据都是严格保序的。

默认情况下，debezium postgres connector针对DML创建独立的topic，且每个表为一个topic

topic命名规则为：

DML topic名称：${database.server.name}.db_name.table_name

DML topic名称均以${database.server.name}开头，因此以前缀方式去正则匹配合并topic，将DML topic进行路由合并为一个topic，且该topic的分区数只能为1。

source端将数据推送至该topic下，同时sink端配置topics为合并后的topic，用于从kafka抽取数据，从而可保证事务的顺序。

DML topic利用路由转发功能进行合并的配置如下：

Source端：
```
database.server.name=postgres_server

transforms=route
transforms.route.type=org.apache.kafka.connect.transforms.RegexRouter
transforms.route.regex=^postgres_server(.*)
transforms.route.replacement=postgres_server_topic
```

Sink端：
```
topics=postgres_server_topic
```

(5) 新增配置参数说明

| 参数                         | 类型    | 参数说明                                                     |
| ---------------------------- | ------- | ------------------------------------------------------------ |
| commit.process.while.running | boolean | 是否开启迁移进度上报功能，默认为false，表示不开启该功能      |
| source.process.file.path     | String  | 迁移进度文件输出路径，默认在迁移插件同一目录下，在迁移进度上报功能开启后起作用 |
| commit.time.interval         | int     | 迁移进度上报的时间间隔，默认值为1，单位：秒，在迁移进度上报功能开启后起作用 |
| create.count.info.path       | String  | 记录源端有效日志生产总数的文件输出路径，默认在迁移插件同一目录下，必须与sink端的该路径保持一致，用于和sink端交互获取总体同步时延 |
| process.file.count.limit     | int     | 同一目录下文件数目限制，超过该数目工具会按时间从早到晚删除多余进度文件，默认为10 |
| process.file.time.limit      | int     | 进度文件保存时间，超过该时间后工具会删除对应的进度文件，默认为168，单位：小时 |
| append.write                 | boolean | 进度文件写入方式，true表示追加写入，false表示覆盖写入，默认值为false |
| file.size.limit              | int     | 文件大小限制，超过该限制值工具会另启新文件写入，默认为10，单位：兆 |
| min.start.memory             | String  | 自定义配置debezium最小启动内存，通过脚本生效，默认为256M     |
| max.start.memory             | String  | 自定义配置debezium最大启动内存，通过脚本生效，默认为2G       |
| queue.size.limit             | int     | 存储kafka记录的队列的最大长度，int类型，默认值为1000000      |
| open.flow.control.threshold  | double  | 流量控制参数，double类型，默认值为0.8，当存储某一队列长度>最大长度queue.size.limit*该门限值时，将启用流量控制，暂停抽取事件 |
| close.flow.control.threshold | double  | 流量控制参数，double类型，默认值为0.7，当存储各个队列长度<最大长度queue.size.limit*该门限值时，将关闭流量控制，继续抽取事件 |
| wait.timeout.second          | int     | 自定义JDBC连接在被服务器自动关闭之前等待活动的秒数。如果客户端在这段时间内没有向服务器发送任何请求，服务器将关闭该连接，默认值：28800，单位：秒 |
| export.csv.path              | String  | 全量数据采集后写入文件的位置                                 |
| export.file.size             | String  | 全量数据文件大小划分配置，支持K、M、G大小配置，没有单位默认按照M单位处理，默认值为 2M |
| export.csv.path.size         | String  | 文件夹大小控制 支持K、M、G大小配置，没有单位时默认按照G单位处理，默认值为null |
| migration.type               | String  | 迁移类型，无默认值。full:全量迁移,incremental:增量，object:对象 |
| migration.view               | boolean | 对象迁移时生效，是否迁移视图                                 |
| migration.func               | boolean | 对象迁移时生效，是否迁移函数和存储过程                       |
| migration.trigger            | boolean | 对象迁移时生效，是否迁移触发器                               |
| migration.workers            | int     | 全量迁移表数据导出的并发度                                   |
| export.page.number           | int     | 分页抽取一次抽取的分片数量。取值范围为(0, 100000]，默认值50。需根据实际jvm内存进行配置 |
| truncate.handling.mode       | String  | 指定如何处理更改事件的 TRUNCATE操作（仅在 pg11+ pgoutput 插件上受支持），包括skip和include。默认值为skip |
快照点参数配置说明：

case 1: 查询到一个xlog location，配置当前查询的xlog location为快照点。

```
postgres> select * from pg_current_xlog_location(); --pg 10.0以上版本使用select * from pg_current_wal_lsn()；
+-----------------------------+
| pg_current_xlog_location    |
+-----------------------------+
| B/AD7DEA8                   |
+-----------------------------+
(1 row)

```

如上示例中，根据查询到的快照点若配置新增的snapshot相关的参数，需配置为如下：

```
xlog.location=B/AD7DEA8
```

case 2：若和全量迁移对接，快照点从表sch_debezium.pg_replica_tables中获得。

pg_schema_name对应表所属schema，pg_table_name对应表名，lsn与列pg_xlog_location相对应。

```
openGauss=> select * from sch_debezium.pg_replica_tables;

 id | pg_schema_name | pg_table_name | pg_xlog_location
----+----------------+---------------+------------------
  1 | public         | t1            | B/AD7DEA8
(1 row)
```

全量迁移开始时会为每个表生成快照，并保存在sch_debezium.pg_replica_tables，增量迁移启动后会加载每个表的快照信息，作为增量迁移的起始点。
使用表级快照点，需配置如下参数值：

```
slot.drop.on.stop=false
```

#### Sink端

(1) 启动类

```
connector.class=io.debezium.connector.postgresql.sink.PostgresSinkConnector
```
(2) 配置文件示例

[postgres-sink.properties](https://gitee.com/opengauss/debezium/blob/master/debezium-connector-postgres/patch/postgres-sink.properties)

(3) 新增配置参数说明

| 参数                         | 类型    | 参数说明                                                     |
| ---------------------------- | ------- | ------------------------------------------------------------ |
| topics                       | String  | sink端从kafka抽取数据的topic                                 |
| opengauss.driver             | String  | openGauss驱动名                                              |
| opengauss.username           | String  | openGauss用户名                                              |
| opengauss.password           | String  | openGauss用户密码                                            |
| opengauss.url                | String  | openGauss连接url                                             |
| xlog.location                | String  | 增量迁移停止时openGauss端lsn的存储文件路径                   |
| schema.mappings              | String  | postgres和openGauss的schema映射关系，用；区分不同的映射关系，用：区分postgres的schema和openGauss的schema<br>例如<br>schema_mappings:<br/>      postgres_schema1: opengauss_schema1<br/>      postgres_schema2: opengauss_schema2<br/> |
| commit.process.while.running | boolean | 是否开启迁移进度上报功能，默认为false，表示不开启该功能      |
| sink.process.file.path       | String  | 迁移进度文件输出路径，默认在迁移插件同一目录下，在迁移进度上报功能开启后起作用 |
| commit.time.interval         | int     | 迁移进度上报的时间间隔，默认值为1，单位：秒，在迁移进度上报功能开启后起作用 |
| create.count.info.path       | String  | 记录源端有效日志生产总数的文件输出路径，默认在迁移插件同一目录下，必须与source端的该路径保持一致，用于和source端交互获取总体同步时延 |
| fail.sql.path                | String  | 回放失败的sql语句输出路径，默认在迁移插件同一目录下          |
| process.file.count.limit     | int     | 同一目录下文件数目限制，超过该数目工具会按时间从早到晚删除多余进度文件，默认为10 |
| process.file.time.limit      | int     | 进度文件保存时间，超过该时间后工具会删除对应的进度文件，默认为168，单位：小时 |
| append.write                 | boolean | 进度文件写入方式，true表示追加写入，false表示覆盖写入，默认值为false |
| file.size.limit              | int     | 文件大小限制，超过该限制值工具会另启新文件写入，默认为10，单位：兆 |
| queue.size.limit             | int     | 存储kafka记录的队列的最大长度，int类型，默认值为1000000      |
| open.flow.control.threshold  | double  | 流量控制参数，double类型，默认值为0.8，当存储kafka记录的队列长度>最大长度queue.size.limit*该门限值时，将启用流量控制，暂停从kafka抽取数据 |
| close.flow.control.threshold | double  | 流量控制参数，double类型，默认值为0.7，当存储kafka记录的队列长度<最大长度queue.size.limit*该门限值时，将关闭流量控制，继续从kafka抽取数据 |
| wait.timeout.second          | long    | ink端数据库停止服务后迁移工具等待数据库恢复服务的最大时长，默认值：28800，单位：秒 |
| delete.full.csv.file         | boolean | 控制在全量数据迁移处理完csv文件后是否删除文件，默认值false   |
| create.table.with.options    | String  | 建表添加with选项，JsonArray形式，table为表名，with选项值之间用","隔开，默认值：[] |
### 迁移进度上报信息说明

#### Source端

| 参数                           | 参数说明                           |
|------------------------------|--------------------------------|
| timestamp                       | source端当前上报信息的时间戳              |
| createCount             | 生产事务数            |
| skippedExcludeCount             | source端跳过的黑名单之内或白名单之外的变更数      |
| convertCount | 完成解析的事务数 |
| pollCount | 存入kafka的事务数 |
| rest | source端剩余事务数（已生产但未存入kafka的事务数） |
| speed | source端处理速度（每秒处理的事务数） |
#### Sink端

| 参数                     | 参数说明                             |
| ------------------------ | ------------------------------------ |
| timestamp                | sink端当前上报信息的时间戳           |
| extractCount             | 从kafka抽取的事务数                  |
| skippedCount             | 跳过的事务数                         |
| replayedCount            | 已回放事务总数                       |
| successCount             | 回放成功的事务数                     |
| failCount                | 回放失败的事务数                     |
| skippedExcludeEventCount | 跳过的黑名单表的事务数               |
| rest                     | 剩余事务数（已抽取但未回放的事务数） |
| speed                    | sink端处理速度（每秒处理的事务数）   |
| overallPipe              | 当前时间片处于迁移管道中的事务总数   |


## 基于Debezium postgres connector进行数据迁移

### 环境依赖

kafka，zookeeper，confluent community，debezium-connector-postgres

### 原理

**全量迁移：**debezium postgres connector的source端首先获取指定的表，通过pgjdbc获取源端表结构信息，推送至kafka中缓存；然后获取表的快照，对源端表数据进行并发抽取，将抽取到的数据转换成csv格式写入文件，且向kafka推送文件路径以及分片消息；表数据抽取完成后抽取表的索引信息，并向kafka推送；sink端从kafka中持续的拉取消息，并进行输入导入操作。

**对象迁移**：source端通过jdbc获取源库对应的所有对象信息，并组装成ddl推送至kafka缓存；sink端从kafka中持续的拉取消息，并进行对象的创建。

**增量迁移**：debezium postgres connector的source端，监控postgres数据库的wal日志，并将数据以AVRO格式写入到kafka；debezium postgres connector的sink端，从kafka读取AVRO格式数据，并组装为事务，在openGauss端按照表级粒度并行回放，从而完成数据从postgres在线迁移至openGauss端。

由于该方案严格保证事务的顺序性，因此将DML路由在kafka的一个topic下，且该topic的分区数只能为1(参数num.partitions=1)，从而保证source端推送到kafka，和sink端从kafka拉取数据都是严格保序的。

### 约束及限制

(1) PostgreSQL 9.4.26及以上版本：

PostgreSQL版本低于10.x，仅支持使用wal2json插件创建逻辑复制槽；
PostgreSQL版本大于10.x，支持使用wal2json和pgoutput插件创建逻辑复制槽（推荐使用pgoutput插件）。


(2) PostgreSQL开启逻辑复制功能：

配置参数
```
wal_level=logical
```
(3) 仅限初始用户和拥有REPLICATION权限的用户进行操作。三权分立关闭时数据库管理员可以进行逻辑复制操作，三权分立开启时不允许数据库管理员进行逻辑复制操作；

(4) PostgreSQL的库与逻辑复制槽一一对应，当待迁移的库改变时，需要配置新的逻辑复制槽的名字；

(5) 初次使用时要以sysadmin权限的用户开启工具。

(6) 在线迁移直接透传DML，对于openGauss和PostgreSQL不兼容的语法，迁移会报错；

(7) Kafka中以AVRO格式存储数据，AVRO字段名称[命名规则](https://avro.apache.org/docs/1.11.1/specification/#names)为：
```
- 以[A-Za-z_]开头
- 随后仅包含[A-Za-z0-9_]
```
因此，对于PostgreSQL中的标识符，表名和列名推荐按上述规范命名，否则在线迁移可能会报错。

(8) 增量迁移过程中创建的表不会进行迁移，如有需要，请重启迁移工具。

(9)  全量迁移支持迁移主键约束和非空约束。暂不支持迁移外键，check约束和唯一约束。

(10) 全量迁移暂不支持列comment信息，字符集和字符序的迁移。

(11) 全量迁移支持仅支持迁移一级分区，支持的分区表类型如下：

| pg分区类型 | og分区类型 |
| ---------- | ---------- |
| range分区  | range分区  |
| list分区   | list分区   |
| hash分区   | hash分区   |

(12) 对象迁移仅支持函数、视图、触发器、存储过程的迁移，不支持用户、权限及序列。

(13) 全量迁移需设置source端参数snapshot.mode为always。

(14) 不支持自定义数据类型的迁移，若源端存在自定义数据类型，需要在目标端提前创建。

(15) 其他迁移场景依赖于openGauss数据库与postgresql数据库的兼容情况。

### 数据类型映射

postgresql数据迁移至openGauss数据类型映射关系如下：

| pg类型                                  | og类型                                  |
| --------------------------------------- | --------------------------------------- |
| smallint                                | smallint                                |
| integer                                 | integer                                 |
| bigint                                  | bigint                                  |
| decimal                                 | decimal                                 |
| numeric                                 | numeric                                 |
| real                                    | real                                    |
| double precision                        | double precision                        |
| smallserial                             | smallserial                             |
| serial                                  | serial                                  |
| bigserial                               | bigserial                               |
| money                                   | money                                   |
| varchar(n)   character varying(n)       | varchar(n)   character varying(n)       |
| char(n), character(n)                   | char(n), character(n)                   |
| varchar                                 | varchar                                 |
| char                                    | char                                    |
| text                                    | text                                    |
| name                                    | name                                    |
| bytea                                   | bytea                                   |
| timestamp [ (p) ] [ without time zone ] | timestamp [ (p) ] [ without time zone ] |
| timestamp [ (p) ] with time zone        | timestamp [ (p) ] with time zone        |
| timestamp  without time zone            | timestamp(6)  without time zone         |
| timestamp  with time zone               | timestamp(6)  with time zone            |
| date                                    | date                                    |
| time [ (p) ] [ without time zone ]      | time [ (p) ] [ without time zone ]      |
| time [ (p) ] with time zone             | time [ (p) ] with time zone             |
| time without time zone                  | time(6) without time zone               |
| time with time zone                     | time(6) with time zone                  |
| interval [ fields ] [ (p) ]             | interval [ fields ] [ (p) ]             |
| interval                                | interval(6)                             |
| boolean                                 | boolean                                 |
| oid                                     | oid                                     |
| enum                                    | enum                                    |
| point                                   | point                                   |
| line                                    | varchar                                 |
| lseg                                    | lseg                                    |
| box                                     | box                                     |
| path                                    | path                                    |
| polygon                                 | polygon                                 |
| circle                                  | circle                                  |
| cidr                                    | cidr                                    |
| inet                                    | inet                                    |
| macaddr                                 | macaddr                                 |
| bit                                     | bit(1)                                  |
| bit(n)                                  | bit(n)                                  |
| bit varying(n)                          | bit varying(n)                          |
| tsvector                                | tsvector                                |
| tsquery                                 | tsquery                                 |
| uuid                                    | uuid                                    |
| xml                                     | xml                                     |
| json                                    | json                                    |
| jsonb                                   | jsonb                                   |
| array                                   | array                                   |
| Composite Types（组合类型）             | Composite Types                         |
| int4range                               | int4range                               |
| int8range                               | int8range                               |
| numrange                                | numrange                                |
| tsrange                                 | tsrange                                 |
| tstzrange                               | tstzrange                               |
| daterange                               | daterange                               |
| 域类型                                  | 域类型                                  |
| pg_lsn                                  | varchar                                 |

### 性能指标

**全量迁移：**基于sysbench测试模型，2路鲲鹏920 CPU、openEuler操作系统下，Postgresql数据库20张表（无索引）单表数据量在500万以上时，使用20并发迁移数据至openGauss，整体全量迁移性能可达300M/s以上。

**增量迁移：**按表回放，利用sysbench进行测试，在openEuler arm操作系统2p Kunpeng-920机器，针对混合IUD场景，50张表50个线程（insert-30线程，update-10线程，delete-10线程），性能可达3w tps。

### 部署过程

#### 下载依赖

- [kafka](https://mirrors.tuna.tsinghua.edu.cn/apache/kafka/)
  （以kafka_2.13-3.6.1为例）
  ```
  wget -c https://mirrors.tuna.tsinghua.edu.cn/apache/kafka/3.6.1/kafka_2.13-3.6.1.tgz  
  
  tar -zxf kafka_2.13-3.6.1.tgz 
  ```

- [confluent community](https://packages.confluent.io/archive/5.5/confluent-community-5.5.1-2.12.zip)

  ```
  wget -c  https://packages.confluent.io/archive/5.5/confluent-community-5.5.1-2.12.zip
  
  unzip confluent-community-5.5.1-2.12.zip
  ```

- [debezium-connector-postgres](https://opengauss.obs.cn-south-1.myhuaweicloud.com/latest/tools/replicate-postgresql2openGauss-7.0.0rc3.tar.gz)

  ```
  wget -c https://opengauss.obs.cn-south-1.myhuaweicloud.com/latest/tools/replicate-postgresql2openGauss-7.0.0rc3.tar.gz

  tar -zxvf replicate-postgresql2openGauss-7.0.0rc3.tar.gz
  ```

#### 修改配置文件

默认配置文件的地址均为localhost，若需修改为具体ip，请同步修改下列所有文件中参数涉及localhost的均改为实际ip。

- zookeeper

  ```
  配置文件位置：/kafka_2.13-3.6.1/config/zookeeper.properties
  ```
  zookeeper的默认端口号为2181，对应参数clientPort=2181。

  若端口冲突，需要修改端口号，则同步修改以下文件对应参数：
  ```
  kafka_2.13-3.6.1/config/zookeeper.properties------clientPort=2181
  kafka_2.13-3.6.1/config/server.properties------zookeeper.connect=localhost:2181
  confluent-5.5.1/etc/schema-registry/schema-registry.properties------kafkastore.connection.url=localhost:2181
  ```

- kafka

  ```
  配置文件位置：/kafka_2.13-3.6.1/config/server.properties
  ```

  注意topic的分区数必须为1，因此需设置参数num.partitions=1，该参数默认值即为1，因此无需单独修改该参数。

  kafka的默认端口是9092，对应参数listeners=PLAINTEXT://:9092。

  若端口冲突，需要修改端口号，则同步修改以下文件对应参数：
  ```
  kafka_2.13-3.6.1/config/server.properties------listeners=PLAINTEXT://:9092
  confluent-5.5.1/etc/schema-registry/schema-registry.properties------kafkastore.bootstrap.servers=PLAINTEXT://localhost:9092
  confluent-5.5.1/etc/schema-registry/connect-avro-standalone.properties------bootstrap.servers=localhost:9092
  confluent-5.5.1/etc/kafka/postgres-source.properties------database.history.kafka.bootstrap.servers=127.0.0.1:9092
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
  若需查看kafka topic内容，需修改
  confluent-5.5.1/bin/kafka-avro-console-consumer------DEFAULT_SCHEMA_REGISTRY_URL="--property schema.registry.url=http://192.168.0.219:8081"
  ```

- connect-standalone

  ```
  配置文件位置：/confluent-5.5.1/etc/schema-registry/connect-avro-standalone.properties
  ```

  注意在plugin.path配置项中增加debezium-connector-postgres所在的路径

  若debezium-connector-postgres所在路径为：/data/debezium_kafka/plugin/debezium-connector-postgres

  则配置其上一层目录，即plugin.path=share/java,/data/debezium_kafka/plugin

  connect-standalone的默认端口是8083，对应参数rest.port=8083。

  若端口冲突，需要修改端口号，则同步修改以下文件对应参数：
  ```
  confluent-5.5.1/etc/schema-registry/connect-avro-standalone.properties------rest.port=8083
  ```

- postgres-source.properties

  ```
  配置文件位置：/confluent-5.5.1/etc/kafka/postgres-source.properties
  ```

  示例详见[postgres-source.properties](https://gitee.com/opengauss/debezium/blob/master/debezium-connector-postgres/patch/postgres-source.properties)


- postgres-sink.properties

  ```
  配置文件位置：/confluent-5.5.1/etc/kafka/postgres-sink.properties
  ```

  示例详见[postgres-sink.properties](https://gitee.com/opengauss/debezium/blob/master/debezium-connector-postgres/patch/postgres-sink.properties)

#### 启动命令

（1）启动zookeeper

```
cd kafka_2.13-3.6.1
./bin/zookeeper-server-start.sh ./config/zookeeper.properties
```

（2）启动kafka

```
cd kafka_2.13-3.6.1
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
./bin/connect-standalone etc/schema-registry/connect-avro-standalone.properties etc/kafka/postgres-source.properties
```

（5）启动kafka-connect sink端

```
cd confluent-5.5.1
./bin/connect-standalone etc/schema-registry/connect-avro-standalone-1.properties etc/kafka/postgres-sink.properties
```
说明：source端和sink端的两个配置文件connect-avro-standalone.properties和connect-avro-standalone-1.properties的差异点在于rest.port参数的不同，默认为8083，即两个文件中设置不同的端口号，即可启动多个kafka-connect，实现sink端和source端独立工作。

步骤(4)和(5)示例source端和sink端分开启动，推荐分开启动方式。两者也可以同时启动，同时启动的命令为：

```
cd confluent-5.5.1
./bin/connect-standalone etc/schema-registry/connect-avro-standalone.properties etc/kafka/postgres-source.properties etc/kafka/postgres-sink.properties
```

其他命令：

（1）查看topic

```
cd kafka_2.13-3.6.1
./bin/kafka-topics.sh --bootstrap-server 127.0.0.1:9092 --list
```

（2）查看topic的信息

```
cd kafka_2.13-3.6.1
./bin/kafka-topics.sh --bootstrap-server 127.0.0.1:9092 --describe --topic topic_name
```

（3）查看topic的内容

```
cd confluent-5.5.1
./bin/kafka-avro-console-consumer --bootstrap-server 127.0.0.1:9092 --topic topic_name --from-beginning
```

### 全量与增量迁移的配合

(1) 启动全量迁移

启动source端开始前，需首先启动zookeeper，kafka，并注册schema。

debezium-connector-postgres支持全量迁移，可实现表，数据，函数，存储过程，视图，触发器的离线迁移

全量迁移启动后，可在openGauss端的表sch_debezium.pg_replica_tables中查询到全量迁移的快照点，单个表的快照点存储在
sch_debezium.pg_replica_tables中。

(2) 启动增量迁移source端

source端启动后会从sch_debezium.pg_replica_tables加载全量迁移的快照点。无需等待全量迁移结束后
才可启动source端。全量迁移启动后，即可启动source端，这样可以尽可能减少source端的时延，以达到减少迁移延迟的目的。

当然，也可以等待全量迁移结束后启动source端。

(3) 全量迁移结束，启动增量迁移sink端

等待全量迁移结束后，即可启动sink端回放数据。

针对全量迁移的表，若对其的DML事务位于表的快照点之前，将跳过对应的DML操作，避免数据出现重复，可保证迁移过程中数据不丢失，不重复。

若在全量迁移未结束时，就启动sink端，将会导致数据乱序，属于不合理的操作步骤，实际操作过程应避免不合理的操作。

### 性能测试

#### 配置条件
(1) PostgreSQL

- PostgreSQL参数配置：
  ```
  wal_level=logical
  ```
- pg_xlog、安装目录、数据目录分别部署在3个不同的NVME盘

- PostgreSQL高性能配置

(2) openGauss

- pg_xlog、安装目录、数据目录分别部署在3个不同的NVME盘

- openGauss高性能配置

(3) 在线迁移工具Debezium postgres connector

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

(1) sysbench执行prepare命令，为postgres端准备数据

(2) 启动zookeeper，kafka，注册schema，通过debezium离线迁移数据至openGauss端

(3) 开启在线复制

绑核启动source端
```
cd confluent-5.5.1
numactl -C 32-63 -m 0 ./bin/connect-standalone etc/schema-registry/connect-avro-standalone.properties etc/kafka/postgres-source.properties
```

绑核启动sink端
```
cd confluent-5.5.1
numactl -C 64-95 -m 0 ./bin/connect-standalone etc/schema-registry/connect-avro-standalone-1.properties etc/kafka/postgres-sink.properties
```
(4) sysbench执行run命令，给postgres压入数据

按事务并发时，混合IUD场景，10张表50个线程（insert-30线程，update-10线程，delete-10线程）
按表并发时，混合IUD场景，50张表50个线程（insert-30线程，update-10线程，delete-10线程），性能可达3w tps。

(5) 统计迁移工具日志，得到迁移效率



## Debezium opengauss connector

### 功能介绍

新增的debezium opengauss connector作为source端，可用于捕获数据变更并存入kafka。在此基础上添加sink端功能，功能点如下：

- 支持openGauss端对schema下的数据的dml操作同步到MySQL端（不包括分区表），不支持迁移ddl操作的迁移；
- 支持openGauss端对schema下的数据的dml和ddl操作同步到PostgreSQL端（不包括分区表）；
- Sink端支持数据按表进行并发回放；
- 支持openGausss的多个schema下的数据迁移到指定的MySQL的多个库（不包括分区表）；
- 支持openGausss的多个schema下的数据迁移到指定的PostgreSQL的多个schema（不包括分区表）；
- 增加迁移进度上报功能，可用于读取数据迁移时延；
- 增加反向迁移断点续传功能，用户中断后基于断点重启后继续迁移。

### 新增配置参数说明

#### Source端

```
connector.class=io.debezium.connector.opengauss.OpengaussConnector
```

| 参数                           | 类型      | 参数说明                                                               |
|------------------------------|---------|--------------------------------------------------------------------|
| xlog.location                | String  | 自定义配置xlog的位置                                                       |
| wal.sender.timeout           | int     | 自定义数据库等待迁移工具接收日志的最大等待时间，对于openGauss 3.0.x版本，此值默认为12000,单位：毫秒       |
| commit.process.while.running | boolean | 是否开启迁移进度上报功能，默认为false，表示不开启该功能                                     |
| source.process.file.path     | String  | 迁移进度文件输出路径，默认在迁移插件同一目录下，在迁移进度上报功能开启后起作用                            |
| commit.time.interval         | int     | 迁移进度上报的时间间隔，默认值为1，单位：秒，在迁移进度上报功能开启后起作用                             |
| create.count.info.path       | String  | 源端xlog日志写入的dml操作总数，默认在迁移插件同一目录下，必须与sink端的该路径保持一致，用于和sink端交互获取总体同步时延 |
| process.file.count.limit     | int     | 同一目录下文件数目限制，超过该数目工具会按时间从早到晚删除多余进度文件，默认为10                          |
| process.file.time.limit      | int     | 进度文件保存时间，超过该时间后工具会删除对应的进度文件，默认为168，单位：小时                           |
| append.write                 | boolean | 进度文件写入方式，true表示追加写入，false表示覆盖写入，默认值为false                          |
| file.size.limit              | int     | 文件大小限制，超过该限制值工具会另启新文件写入，默认为10，单位：兆                                 |
| min.start.memory             | String  | 自定义配置debezium最小启动内存，通过脚本生效，默认为256M                                 |
| max.start.memory             | String  | 自定义配置debezium最大启动内存，通过脚本生效，默认为2G                                   |

##### 全量数据迁移新增参数

| 参数                           | 类型     | 参数说明                                                                |
|------------------------------|--------|---------------------------------------------------------------------|
| export.csv.path            | String | 全量数据采集后写入文件的位置                                                        |
| export.file.size           | String | 全量数据文件大小划分配置，支持K、M、G大小配置，没有单位默认按照M单位处理，默认值为 2M        |
| export.csv.path.size           | String | 文件夹大小控制 支持K、M、G大小配置，没有单位时默认按照G单位处理，默认值为null        |

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

黑白名单配置说明：

sink connector 可通过配置库级和表级的黑白名单，实现对特定库和表的变更日志的抽取，具体如下：

| 参数                    | 类型     | 参数说明                                                                                         |
|-----------------------|--------|----------------------------------------------------------------------------------------------|
| database.include.list | String | 库级白名单列表，以逗号分隔，规定connector只抽取白名单中的数据库的变更日志                                                    |
| database.exclude.list | String | 库级黑名单列表，以逗号分隔，规定connector不抽取黑名单中的数据库的变更日志                                                    |
| table.include.list    | String | 表级白名单列表，以逗号分隔，规定connector只抽取白名单中的表的变更日志，格式为database_name.table_name 或者schema_name.table_name |
| table.exclude.list    | String | 表级黑名单列表，以逗号分隔，规定connector不抽取黑名单中的表的变更日志，格式为database_name.table_name 或者schema_name.table_name  |

使用说明：
（1）同级的黑白名单不能同时配置；
（2）若要重新配置参数，使得白名单范围变大或黑名单范围变小，需修改source端配置项database.server.name, transforms.route.regex为新的值，注意参数之间的对应关系。

反向增量DDL迁移只支持行存表，不支持列存和Ustore存储引擎，支持迁移的DDL操作如下：

| DDL操作                                   | 备注                           |
|-----------------------------------------|------------------------------|
| CREATE/DROP/ALTER/TRUNCATE/RENAME TABLE | 语法不兼容，不支持分区表                 |
| CREATE/ALTER/DROP INDEX                 | 逻辑解码限制，不支持分区索引/隐藏索引/重建索引     |
| CREATE/ALTER/DROP VIEW                  | 逻辑解码限制，ALTER VIEW仅支持RENAME   |
| CREATE/ALTER/DROP TYPE                  | 逻辑解码限制，ALTER TYPE 仅支持RENAME  |
| CREATE/DROP FUNCTION                    |                              |
| CREATE/DROP TRIGGER                     |                              |
| CREATE/ALTER/DROP SCHEMA                | 逻辑解码限制，ALTER SCHEMA仅支持RENAME |
| CREATE/ALTER/DROP SEQUENCE              |                              |
| COMMENT                                 |                              |
TYPE类型仅支持复合类型和枚举类型


#### Sink端

```
connector.class=io.debezium.connector.opengauss.sink.OpengaussSinkConnector
```

| 参数                                        | 类型      | 参数说明                                                                                                                                                                                                                                                                                                                                                                     |
|-------------------------------------------|---------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| topics                                    | String  | sink端从kafka抽取数据的topic                                                                                                                                                                                                                                                                                                                                                    |
| max.thread.count                          | String  | 自定义Sink端按表回放时的最大并发线程数（不能为0）                                                                                                                                                                                                                                                                                                                                              |
| database.type                             | String  | 数据库类型，当前支持mysql,postgresql,opengauss和oracle，默认值为mysql                                                                                                                                                                                                                                                                                                                    |
| database.username                         | String  | 数据库用户名                                                                                                                                                                                                                                                                                                                                                                   |
| database.password                         | String  | 数据库用户密码                                                                                                                                                                                                                                                                                                                                                                  |
| database.ip                               | String  | 数据库ip                                                                                                                                                                                                                                                                                                                                                                    |
| database.port                             | int     | 数据库端口                                                                                                                                                                                                                                                                                                                                                                    |
| database.name                             | String  | 数据库名称                                                                                                                                                                                                                                                                                                                                                                    |
| schema.mappings                           | String  | openGauss的schema与目标端database或者schema的映射关系，与全量迁移chameleon配置相反，用；区分不同的映射关系，用：区分openGauss的schema与目标数据库的database或者schema<br>例如chameleon的配置<br>schema_mappings:<br/>      mysql_database1: opengauss_schema1<br/>      mysql_database2: opengauss_schema2<br/>则sink端的schema.mappings参数需配置为schema.mappings=opengauss_schema1:mysql_database1;opengauss_schema2:mysql_database2 |
| commit.process.while.running              | boolean | 是否开启迁移进度上报功能，默认为false，表示不开启该功能                                                                                                                                                                                                                                                                                                                                           |
| sink.process.file.path                    | String  | 迁移进度文件输出路径，默认在迁移插件同一目录下，在迁移进度上报功能开启后起作用                                                                                                                                                                                                                                                                                                                                  |
| commit.time.interval                      | int     | 迁移进度上报的时间间隔，默认值为1，单位：秒，在迁移进度上报功能开启后起作用                                                                                                                                                                                                                                                                                                                                   |
| create.count.info.path                    | String  | 源端xlog日志写入的dml操作总数，默认在迁移插件同一目录下，必须与source端的该路径保持一致，用于和source端交互获取总体同步时延                                                                                                                                                                                                                                                                                                  |
| fail.sql.path                             | String  | 回放失败的sql语句输出路径，默认在迁移插件同一目录下                                                                                                                                                                                                                                                                                                                                              |
| process.file.count.limit                  | int     | 同一目录下文件数目限制，超过该数目工具会按时间从早到晚删除多余进度文件，默认为10                                                                                                                                                                                                                                                                                                                                |
| process.file.time.limit                   | int     | 进度文件保存时间，超过该时间后工具会删除对应的进度文件，默认为168，单位：小时                                                                                                                                                                                                                                                                                                                                 |
| append.write                              | boolean | 进度文件写入方式，true表示追加写入，false表示覆盖写入，默认值为false                                                                                                                                                                                                                                                                                                                                |
| file.size.limit                           | int     | 文件大小限制，超过该限制值工具会另启新文件写入，默认为10，单位：兆                                                                                                                                                                                                                                                                                                                                       |
| queue.size.limit                          | int     | 存储kafka记录的队列和按表并发线程中预处理数据的队列的最大长度，int类型，默认值为1000000                                                                                                                                                                                                                                                                                                                      |
| open.flow.control.threshold               | double  | 流量控制参数，double类型，默认值为0.8，当存储kafka记录的队列或某一个按表并发线程中预处理数据的队列长度>最大长度queue.size.limit*该门限值时，将启用流量控制，暂停从kafka抽取数据                                                                                                                                                                                                                                                               |
| close.flow.control.threshold              | double  | 流量控制参数，double类型，默认值为0.7，当存储kafka记录的队列和所有按表并发线程中预处理数据的队列长度<最大长度queue.size.limit*该门限值时，将关闭流量控制，继续从kafka抽取数据                                                                                                                                                                                                                                                                |
| record.breakpoint.kafka.topic             | String  | 自定义断点记录topic，在回放过程记录执行结果到Kafka中，可根据实际情况修改，默认值为bp_topic                                                                                                                                                                                                                                                                                                                   |
| record.breakpoint.kafka.bootstrap.servers | String  | 自定义断点记录的Kafka启动服务器地址，如无特殊需要，配置为source端的Kafka地址，可根据实际情况修改，默认值为localhost:9092                                                                                                                                                                                                                                                                                              |
| record.breakpoint.kafka.attempts          | int     | 自定义读取断点记录重试次数，默认为3                                                                                                                                                                                                                                                                                                                                                       |
| record.breakpoint.kafka.size.limit        | int     | 断点记录Kafka的条数限制，超过该限制会触发删除Kafka的断点清除策略，删除无用的断点记录数据，单位：事务万条数，默认值：3000                                                                                                                                                                                                                                                                                                      |
| record.breakpoint.kafka.clear.interval    | int     | 断点记录Kafka的时间限制，超过该限制会触发删除Kafka的断点清除策略，删除无用的断点记录数据，单位：小时，默认值1                                                                                                                                                                                                                                                                                                             |
| record.breakpoint.repeat.count.limit      | int     | 断点续传时，查询待回放数据是否已在断点之前备回放的数据条数，默认值：50000                                                                                                                                                                                                                                                                                                                                  
| wait.timeout.second                       | long    | sink端数据库停止服务后迁移工具等待数据库恢复服务的最大时长，默认值：28800，单位：秒                                                                                                                                                                                                                                                                                                                           

##### 全量数据迁移新增参数

| 参数                           | 类型   | 参数说明                                                                                                                                                                                                                                                                                                                                               |
|------------------------------| ------ |----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| delete.full.csv.file                       | boolean | 控制在全量数据迁移处理完csv文件后是否删除文件，默认值false.                                                                                                                                                                                                                                                                                                                              |

### 迁移进度上报信息说明

#### Source端

| 参数                    | 参数说明                                      |
| ----------------------- | --------------------------------------------- |
| timestamp               | source端当前上报信息的时间戳                  |
| createCount             | 生产数据量（写入xlog的数据量）                |
| convertCount            | 完成解析数据量                                |
| pollCount               | 存入kafka的数据量                             |
| skippedExcludeDataCount | 跳过的黑名单表的数据量                        |
| rest                    | 剩余数据量（已写入xlog但未存入kafka的数据量） |
| speed                   | source端处理速度（每秒处理的数据量）          |

#### Sink端

| 参数          | 参数说明                           |
| ------------- | ---------------------------------- |
| timestamp     | sink端当前上报信息的时间戳         |
| extractCount  | 从kafka抽取的数据量                |
| replayedCount | 已完成回放的数据量                 |
| successCount  | 回放成功的数据量                   |
| failCount     | 回放失败的数据量                   |
| rest          | 剩余数据量（已抽取但未回放）       |
| speed         | sink端处理速度（每秒处理的数据量） |
| overallPipe   | 当前时间片处于迁移管道中的数据总数 |

#### 全量数据迁移进度上报

```
{
    // 各表进度信息
    "table": [
        {
            "name": "sbtest1",  // 迁移表的名称
            "percent": 1.0,     // 迁移百分比，1.0代表迁移完成，0.5代表迁移了50%
            "status": 3         // 迁移状态
        },
        {
            "name": "sbtest2",
            "percent": 1.0,
            "status": 3
        }
    ],
    "total": {              // 总体进度
        "record": 500000,   // 总记录数，迁移全量数据的总行数
        "data": "184.00",   // 迁移总大小，默认单位 M。
        "time": 19,         // 迁移时间，默认单位 s。
        "speed": "10.22"    // 迁移的速率，默认单位 MB/s。
    }
}
```

## 基于Debezium opengauss connector进行反向迁移

### 环境依赖

kafka， zookeeper，confluent community，debezium-connector-opengauss

### 原理

debezium opengauss connector的source端，监控openGauss数据库的逻辑日志，并将数据写入到kafka；debezium opengauss connector的sink端，从kafka读取数据，并组装为sql语句，在sink端按表并行回放，从而完成数据从openGauss在线迁移至sink端。

#### 全量数据迁移原理

debezium opengauss connector的source端，采集表的全量数据，按照数据量划分数据写入文件，将文件路径及其表信息推送到kafka中；debezium opengauss connector的sink端，消费kafka中的消息，读取信息，将文件数据加载进内存进行数据转换，并组装为sql语句，在sink端按表并行回放，从而完成数据从openGauss迁移至sink端。

### 前置条件

openGauss开启逻辑复制功能：

    （1）仅限初始用户和拥有REPLICATION权限的用户进行操作。三权分立关闭时数据库管理员可以进行逻辑复制操作，三权分立开启时不允许数据库管理员进行逻辑复制操作；
    （2）openGauss的库与逻辑复制槽一一对应，当待迁移的库改变时，需要配置新的逻辑复制槽的名字；
    （3）初次使用时要以sysadmin权限的用户开启工具。

openGauss参数配置：

```
wal_level=logical
```
约束：

    （1）反向迁移sink端按表分发数据，不支持按事务分发，日志中记录的回放条数为实际成功执行的sql语句条数，openGauss分区表执行update操作时，如果更新前的数据和更新后的数据在同一分区，只会执行一条update语句，如果不在同一分区，会以事务的形式先后执行一条delete语句和一条insert语句，这种情形下日志会显示回放了两条数据；
    （2）反向迁移connector端配置连接数据库的用户需要有对应数据库下所有schema以及所有表的操作权限
    （3）反向迁移数据类型映射与变色龙的默认数据类型映射相反，当两端数据类型不一致时，只能迁移两端数据类型都支持的数据变更
    （4）MySQL在迁移binary, varbinary, blob, tinyblob, blob, mediumblob, longblob类型时，需设置源端数据库参数dolphin.b_compatibility_mode=on

### 部署过程

#### 下载依赖

- [kafka](https://mirrors.tuna.tsinghua.edu.cn/apache/kafka/)
  （以kafka_2.13-3.6.1为例）
  ```
  wget -c https://mirrors.tuna.tsinghua.edu.cn/apache/kafka/3.6.1/kafka_2.13-3.6.1.tgz
  
  tar -zxf kafka_2.13-3.6.1.tgz
  ```

- [confluent community](https://packages.confluent.io/archive/5.5/confluent-community-5.5.1-2.12.zip)

  ```
  wget -c  https://packages.confluent.io/archive/5.5/confluent-community-5.5.1-2.12.zip
  
  unzip confluent-community-5.5.1-2.12.zip
  ```

- [debezium-connector-opengauss](https://opengauss.obs.cn-south-1.myhuaweicloud.com/latest/tools/replicate-openGauss2mysql-7.0.0rc3.tar.gz)

  ```
  wget -c https://opengauss.obs.cn-south-1.myhuaweicloud.com/latest/tools/replicate-openGauss2mysql-7.0.0rc3.tar.gz
  
  tar -zxvf replicate-openGauss2mysql-7.0.0rc3.tar.gz
  ```

#### 修改配置文件

- zookeeper

  ```
  配置文件位置：/kafka_2.13-3.6.1/config/zookeeper.properties
  ```

- kafka

  ```
  配置文件位置：/kafka_2.13-3.6.1/config/server.properties
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
  反向增量迁移开关
  snapshot.mode=never
  全量数据迁移开关
  snapshot.mode=always
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
cd kafka_2.13-3.6.1
./bin/zookeeper-server-start.sh ./config/zookeeper.properties
```

（2）启动kafka

```
cd kafka_2.13-3.6.1
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
cd kafka_2.13-3.6.1
./bin/kafka-topics.sh --bootstrap-server 127.0.0.1:9092 --list
```

（2）查看topic的内容

```
cd confluent-5.5.1
./bin/kafka-avro-console-consumer --bootstrap-server 127.0.0.1:9092 --topic topic_name --from-beginning
```

#### 性能测试模型

利用sysbench进行测试，在openEuler arm操作系统2p Kunpeng-920机器，针对混合IUD场景，50张表50个线程（insert-30线程，update-10线程，delete-10线程），性能可达1w tps。

## FAQ

### (1) schema-registry报错

schema-registry报错: `Schema being registered is incompatible with an earlier schema`

解决方案：
停止schema-registry进程，执行下面curl命令，并重新启动schema-registry和kafka-connect

可根据实际配置修改ip:localhost和端口:8081

```
curl -X GET http://localhost:8081/config

curl -X PUT -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data '{"compatibility": "NONE"}' \
  http://localhost:8081/config
```

### (2) kafka消息过大

kafka消息过大：`org.apache.kafka.common.errors.RecordTooLargeException`

```
The message is 2110235 bytes when serialized which is larger than 1048576, which is the value of the max.request.size configuration.
The request included a message larger than the max message size the server will accept.
```

解决方案：

kafka默认配置对发送的消息大小有一定的限制，默认为1M。若需发送大消息，需同步修改kafka，producer和consumer端的参数，以允许发送大消息。

kafka，producer，consumer相关的参数详情请参考[参数说明](https://docs.confluent.io/platform/current/installation/configuration/index.html)。

对于kafka connect，若需重写producer或者consumer默认的worker线程相关的配置，请添加对应的producer/consumer.override前缀，详情请参考[参数重写](https://docs.confluent.io/platform/current/connect/references/allconfigs.html#override-the-worker-configuration)。

即对于producer，参数重写添加`producer.override`前缀；对于consumer，参数重写添加`consumer.override`前缀。

参数重写生效的前提需配置参数

```
connector.client.config.override.policy=All
```

该参数配置在`confluent-5.5.1/etc/schema-registry/connect-avro-standalone.properties`文件中。

针对kafka发送large message，需修改的参数如下，详情请参考[kafka send large message](https://www.baeldung.com/java-kafka-send-large-message)：

| 参数                      | 默认值            | 参数含义                                     | 所属范围 | 修改方式                                                     |
| ------------------------- | ----------------- | -------------------------------------------- | -------- | ------------------------------------------------------------ |
| max.request.size          | 1048576B，即1MB   | 生产者能发送的消息的最大值                   | producer | 作为producer，在source端修改，source端配置文件增加该参数，需增加producer.override前缀 |
| message.max.bytes         | 1048588B          | kafka broker能接收的消息的最大值             | kafka    | 作为kafka参数，需在配置文件server.properties中增加该参数     |
| fetch.max.bytes           | 52428800B，即50MB | 消费者单次从kafka broker获取消息的最大字节数 | consumer | 作为consumer，在sink端修改，sink端配置文件增加该参数，需增加consumer.override前缀 |
| max.partition.fetch.bytes | 1048576B，即1MB   | 消费者从单个分区获取消息的最大字节数         | consumer | 作为consumer，在sink端修改，sink端配置文件增加该参数，需增加consumer.override前缀 |

举例如下：

若需修改kafka消息为3MB=3145728B，需同步修改如下文件：

- producer

  若为正向迁移，mysql-> openGauss, `mysql-source.properties`文件中增加下述参数；

  若为反向迁移，openGauss -> mysql, `opengauss-source.properties`文件中增加下述参数：

  ```
  producer.override.max.request.size=3145728
  ```

- kafka

  若通过confluent启动kafka，`confluent-5.5.1/etc/kafka/server.properties`文件中增加下述参数；

  若通过kafka安装包启动kafka，`kafka_2.13-3.6.1/config/server.properties`文件中增加下述参数：

  ```
  message.max.bytes=3145728
  ```

  该参数需重启生效

- consumer

  若为正向迁移，mysql-> openGauss, `mysql-sink.properties`文件中增加下述参数；

  若为反向迁移，openGauss -> mysql, `opengauss-sink.properties`文件中增加下述参数：

  ```
  consumer.override.fetch.max.bytes=3145728
  consumer.override.max.partition.fetch.bytes=3145728
  ```

- confluent

  各个启动配置文件`confluent-5.5.1/etc/schema-registry/connect-avro-standalone.properties`中增加参数重写的前置要求：

  ```
  connector.client.config.override.policy=All
  ```

### (3) 迁移过程中同时发生数据变更的表数量超过1000报错

kafka创建的schema数量超过最大值报错：`Too many schema objects created for dml_topic-value!`


解决方案：
kafka默认配置对创建的schema数量有一定的限制，默认为1000。若在迁移过程中同时发生数据变更的表数量超过1000，需修改source和sink端kafka-connect配置文件connect-avro-standalone.properties中参数，以允许创建更多的schema。

在confluent-5.5.1/etc/schema-registry/下`connect-avro-standalone.properties`和`connect-avro-standalone-1.properties`中添加如下两个参数并配置指定的值即可：
```
key.converter.max.schemas.per.subject=1200
value.converter.max.schemas.per.subject=1200
```