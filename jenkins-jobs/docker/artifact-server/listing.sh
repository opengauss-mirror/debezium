#! /usr/bin/env bash

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

OPTS=$(getopt -o d:o: --long dir:,output: -n 'parse-options' -- "$@")
if [ $? != 0 ] ; then echo "Failed parsing options." >&2 ; exit 1 ; fi
eval set -- "$OPTS"

OUTPUT="$(pwd)/artifacts.txt"
while true; do
  case "$1" in
    -d | --dir )                DIR=$2;          shift; shift ;;
    -o | --output )             OUTPUT=$2;                      shift; shift ;;
    -h | --help )               PRINT_HELP=true;                shift ;;
    -- ) shift; break ;;
    * ) break ;;
  esac
done

shopt -s globstar nullglob
pushd "$DIR" || exit
rm -f "$OUTPUT"
for file in **/*.{zip,jar}; do
    prefix=$(echo "$file" | sed -rn 's@^(.*)-([[:digit:]].*([[:digit:]]|Final|SNAPSHOT))(.*)(\..*)$@\1@p')
    suffix=$(echo "$file" | sed -rn 's@^(.*)-([[:digit:]].*([[:digit:]]|Final|SNAPSHOT))(.*)(\..*)$@\4@p')
    artifact="$prefix$suffix"
    echo "$artifact"
    echo "$artifact:$file" >> "$OUTPUT"
done
popd