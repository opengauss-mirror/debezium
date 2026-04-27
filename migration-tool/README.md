# openGauss-FullReplicate 数据迁移工具

## 1 概述

本文档介绍 openGauss-FullReplicate 数据迁移工具的功能特性，并提供安装指南，帮助用户快速上手使用。

## 2 工具简介

openGauss-FullReplicate是一款基于Java开发的高性能数据迁移工具，专注于全量数据及数据库对象的完整迁移。在数据迁移方面，它采用多表并行处理机制，显著提升迁移效率；在对象迁移方面，全面支持索引、约束、主键、外键、视图、函数、触发器、存储过程及序列的迁移，确保目标数据库结构与源端一致。

## 3 支持的迁移场景

| 源数据库   | 目标数据库 | 使用说明                                          |
| :--------- | :--------- | :------------------------------------------------ |
| SQL Server | openGauss  | [查看文档](./docs/sqlserver2opengauss-readme.md)  |
| PostgreSQL | openGauss  | [查看文档](./docs/postgresql2openguass-readme.md) |
| openGauss  | openGauss  | [查看文档](./docs/opengauss2opengauss-readme.md)  |
| Oracle     | oGRAC      | [查看文档](./docs/oracle2ograc-readme.md)         |

## 4 安装指南

### 4.1 环境准备

工具基于 Java 开发，运行前请确保已安装 **Java 17 或更高版本**，检测命令参考如下：

```bash
java -version
```

### 4.2 下载安装包

参考如下命令，下载最新版本安装包：

```bash
wget https://opengauss.obs.cn-south-1.myhuaweicloud.com/latest/tools/openGauss-FullReplicate-7.0.0-RC3.tar.gz
```

下载成功后，得到安装包 `openGauss-FullReplicate-7.0.0-RC3.tar.gz`（`7.0.0-RC3` 为版本号）。

### 4.3 解压安装包

```bash
tar -zxvf openGauss-FullReplicate-7.0.0-RC3.tar.gz
```

### 4.4 验证目录结构

解压后目录结构如下，确认无误即表示安装完成：

```tex
./openGauss-FullReplicate
├── build_commit_id.log
├── openGauss-FullReplicate-7.0.0-RC3.jar
└── config
    ├── config.yml
    └── oracle2ograc_config.yml
```

- `openGauss-FullReplicate-7.0.0-RC3.jar`：工具主程序
- `config/`：配置文件模板目录
