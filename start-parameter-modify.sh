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

# config file path
CONFIG_PATH=/***/***/***/***
min=`sed '/^min.start.memory=/!d;s/.*=//' $CONFIG_PATH`
max=`sed '/^max.start.memory=/!d;s/.*=//' $CONFIG_PATH`
result="\"-Xms${min} -Xmx${max} -XX:+HeapDumpOnOutOfMemoryError\""
KAFKA_PATH=${CONFIG_PATH%/*}
ETC_PATH=${KAFKA_PATH%/*}
CONFLUENT_PATH=${ETC_PATH%/*}
target="${CONFLUENT_PATH}/bin/connect-standalone"
line=`cat $target |grep -E 'export KAFKA_HEAP_OPTS'`
val=${line#*=}
sed -i "s/$val/$result/g" $target
sed -i "s#\r#\\\ #g" $target
echo  success
