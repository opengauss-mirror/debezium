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
    inittmp = 0
    for row in tpcdata:
        if inittmp == 0:
            inittmp = int(row[1])
        db.append(int(row[1]) - inittmp)
        kafka.append(int(row[0]) - inittmp)
        id.append(int(row[3]))


        
xmin=0
xmax=len(id)
ymin=0

del db[0:xmin]
del db[xmax:len(id)]
del kafka[0:xmin]
del kafka[xmax:len(id)]




x = []
y = []

for i in range(xmin,xmax):
    x.append((kafka[i]) / 1000)
    y.append(id[i])
plt.scatter(x,y,s=0.01,c='lightblue')


x = []
y = []
for i in range(xmin,xmax):
    x.append((db[i]) / 1000)
    y.append(id[i])    

axes = plt.gca()
axes.set_xlim([0,150000])
axes.set_ylim([0,100000])
plt.xlabel('millisecond')
plt.ylabel('entries ')


plt.scatter(x,y,s=0.01,c='red')




plt.savefig(Plotfilename)
