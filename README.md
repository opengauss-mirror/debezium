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

对应的patch文件为[mysql-binlog-connector-java-0.25.4.patch][https://gitee.com/opengauss/debezium/tree/master/debezium-connector-mysql/patch/mysql-binlog-connector-java-0.25.4.patch],并在配置文件中增加参数parallel.parse.event参数控制是否启用并行解析event功能，默认为false，即不启用。

同时针对debezium mysql connector，我们也增加sink端能力，可支持数据在openGauss端按照事务粒度并行回放，用于构建完整的端到端的迁移能力（mysql -> openGauss）。

构建Debezium mysql connector, 需应用patch文件，可利用一键式构建脚本[build.sh](https://gitee.com/opengauss/debezium/tree/master/build.sh)快速编译Debezium mysql connector，编译的压缩包的位置为：

```
debezium-connector-mysql/target/debezium-connector-mysql-1.8.1.Final-plugin.tar.gz
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
| parallel.parse.event            | boolean | 是否启用并行解析event能力，默认为false，表示不启用         |

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
| start.replay.flag.position | String | 启动回放接收start信号的文件位置，初次启动connector，需在该文件中接收到start信号，才会开始回放操作 |
| parallel.replay.thread.num | int    | 并行回放默认线程数量，默认为30                               |
| schema.mappings            | String | mysql和openGauss的schema映射关系，与全量迁移chameleon配置相对应，用；区分不同的映射关系，用：区分mysql的database和openGauss的schema<br>例如chameleon的配置<br>schema_mappings:<br/>      mysql_database1: opengauss_schema1<br/>      mysql_database2: opengauss_schema2<br/>则sink端的schema.mappings参数需配置为schema.mappings=mysql_database1:opengauss_schema1;mysql_database2:opengauss_schema2 |

## 基于Debezium mysql connector进行在线迁移

### 环境依赖

kafka， zookeeper，confluent community，debezium-connector-mysql

### 原理

debezium mysql connector的source端，监控mysql数据库的binlog日志，并将数据写入到kafka；debezium mysql connector的sink端，从kafka读取数据，并组装为事务，在openGauss端按照事务粒度并行回放，从而完成数据从mysql在线迁移至openGauss端。

### 前置条件

mysql参数配置：

```
binlog_format=row
binglog_row_image=full
gtid_mode=on #若未开启该参数，则sink端按照事务顺序串行回放
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

（4）启动kafka-connect

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

（2）查看topic的内容

```
cd confluent-5.5.1
./bin/kafka-avro-console-consumer --bootstrap-server 127.0.0.1:9092 --topic topic_name --from-beginning
```
