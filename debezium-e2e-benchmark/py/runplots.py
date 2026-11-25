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

#!/usr/bin/env python3

from kafka import KafkaConsumer
from kafka import KafkaAdminClient
import json
from pprint import pprint
import jaydebeapi
import sys
import random
import string
from pprint import pprint
import requests
import datetime
import threading
import subprocess
import jpype
import os
import shlex


DEBEZIUM_TPC_VOLUME = '/home/tpc/tpcdata'

with open('tpc-config.json') as f:
    tpcconfig = json.load(f)

for x in range(len(tpcconfig['tpc']['commit.intervals'])):
    subprocess.call(shlex.split('python3 tpcplot.py ' + DEBEZIUM_TPC_VOLUME + '/tpc_' + str(tpcconfig['tpc']['count']) + '_' + str(
        tpcconfig['tpc']['commit.intervals'][x]) + '.csv ' + DEBEZIUM_TPC_VOLUME + '/tpc_' + str(tpcconfig['tpc']['count']) + '_' + str(tpcconfig['tpc']['commit.intervals'][x])))
    subprocess.call(shlex.split('python3 time1-tpcplot-full.py ' + DEBEZIUM_TPC_VOLUME + '/tpc_' + str(tpcconfig['tpc']['count']) + '_' + str(
        tpcconfig['tpc']['commit.intervals'][x]) + '.csv ' + DEBEZIUM_TPC_VOLUME + '/tpc_' + str(tpcconfig['tpc']['count']) + '_' + str(tpcconfig['tpc']['commit.intervals'][x]) + '-t'))
    if int(tpcconfig['tpc']['commit.intervals'][x]) < 10:
        zoomfactor = 250
    else:
        zoomfactor = 2.5
        pass
    subprocess.call(shlex.split('python3 time1-tpcplot-detail.py ' + DEBEZIUM_TPC_VOLUME + '/tpc_' + str(tpcconfig['tpc']['count']) + '_' + str(tpcconfig['tpc']['commit.intervals'][x]) + '.csv ' + DEBEZIUM_TPC_VOLUME + '/tpc_' + str(
        tpcconfig['tpc']['count']) + '_' + str(tpcconfig['tpc']['commit.intervals'][x]) + '-t-d ' + str(int(int(tpcconfig['tpc']['commit.intervals'][x]) * zoomfactor)) + ' '))
    subprocess.call(shlex.split('python3 time-delta-hist.py ' + DEBEZIUM_TPC_VOLUME + '/tpc_' + str(tpcconfig['tpc']['count']) + '_' + str(
        tpcconfig['tpc']['commit.intervals'][x]) + '.csv ' + DEBEZIUM_TPC_VOLUME + '/tpc_' + str(tpcconfig['tpc']['count']) + '_' + str(tpcconfig['tpc']['commit.intervals'][x]) + '-h'))
