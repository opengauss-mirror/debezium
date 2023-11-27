#!/bin/bash
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
