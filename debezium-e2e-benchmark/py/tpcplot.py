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
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.pyplot as plt2
import numpy as np
import csv
import sys
from scipy import *

csvfile = sys.argv[1]
Plotfilename = sys.argv[2]

x = []
y = []
db = []
kafka = []
id = []



with open(csvfile) as csvfile:
    tpcdata = csv.reader(csvfile, delimiter=';')
    for row in tpcdata:
        db.append(int(row[1]))
        kafka.append(int(row[0]))
        id.append(int(row[3]))

xmin=0
xmax=len(id)
ymin=0



x = []
y = []

for i in range(xmin,xmax):
    y.append((kafka[i] - db[i]) / 1000)
    x.append(id[i])
ymax=max(y)
plt.subplot(3, 1, 1)
plt.plot(x,y)
plt.xlabel('millisecond')
plt.ylabel('delta insert database to topic')
plt.title('TPC Graph\nInsert data')
plt.legend()
axes = plt.gca()
axes.set_xlim([xmin,xmax])
axes.set_ylim([ymin,ymax])

print('delta insert database to topic')
print('min :',min(y))
print('max :',max(y))
print('average :',sum(y)/len(y))
print('std :',std(y))


x = []
y = []
for i in range(xmin,xmax):
    y.append(db[i])
divy = min(y)
y=[]
for i in range(xmin,xmax):
    y.append((db[i] - divy) / 1000)
    divy=db[i]
    x.append(id[i])

ymax=max(y)
plt.subplot(3, 1, 2)
plt.plot(x,y)
plt.xlabel('entires')
plt.ylabel('delta ')
plt.title('')
plt.legend()
axes = plt.gca()
axes.set_xlim([xmin,xmax])
axes.set_ylim([ymin,40])

print('database in ')
print('min :',min(y))
print('max :',max(y))
print('average :',sum(y)/len(y))
print('std :',std(y))

x = []
y = []
for i in range(xmin,xmax):
    y.append(kafka[i])
divy = min(y)
y=[]
for i in range(xmin,xmax):
    y.append((kafka[i] - divy) / 1000)
    divy=kafka[i]
    x.append(id[i])

ymax=max(y)
plt.subplot(3, 1, 3)
plt.plot(x,y)
plt.xlabel('entires')
plt.ylabel('delta ')
plt.title('')
plt.legend()
axes = plt.gca()
axes.set_xlim([xmin,xmax])

print('kafka in')
print('min :',min(y))
print('max :',max(y))
print('average :',sum(y)/len(y))
print('std :',std(y))


plt.savefig(Plotfilename)
