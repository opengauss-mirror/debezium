#!/bin/bash

# Copyright 2026 Huawei Technologies Co., Ltd.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

mkdir temp
cd ./temp
git clone https://github.com/osheroff/mysql-binlog-connector-java.git -b 0.25.4
cd mysql-binlog-connector-java
cp ../../debezium-connector-mysql/patch/mysql-binlog-connector-java-0.25.4.patch .
git apply --stat mysql-binlog-connector-java-0.25.4.patch
git apply --check mysql-binlog-connector-java-0.25.4.patch
git am -s < mysql-binlog-connector-java-0.25.4.patch
mvn clean package -Dmaven.test.skip=true
mvn -U install:install-file -DgroupId=com.zendesk -DartifactId=mysql-binlog-connector-java -Dversion=0.25.4-modified -Dpackaging=jar -Dfile=./target/mysql-binlog-connector-java-0.25.4-modified.jar -Denforcer.skip=true
cd ../../
rm -rf temp
mvn clean package -P quick,skip-integration-tests,oracle,jdk11,assembly,xstream,xstream-dependency,skip-tests -Dgpg.skip -Dmaven.test.skip=true -Denforcer.skip=true