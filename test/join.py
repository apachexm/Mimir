#!/bin/python

import sys, os, glob
from itertools import chain
from collections import defaultdict

indir1 = sys.argv[1]
indir2 = sys.argv[2]
outfile = sys.argv[3]

dict1 = defaultdict(list)
dict2 = defaultdict(list)

for filename in glob.glob(indir1+'/*'):
    with open(filename, "r") as ins:
        for line in ins:
            line = line.replace('\n', '')
            words = line.split(' ')
            dict1[words[0]].append(words[1])

for filename in glob.glob(indir2+'/*'):
    with open(filename, "r") as ins:
        for line in ins:
            line = line.replace('\n', '')
            words = line.split(' ')
            dict2[words[0]].append(words[1])

of = open(outfile, 'w')
dict3 = defaultdict(list)
for k, mv1 in dict1.items():
    if k in dict2:
        mv2 = dict2[k]
        for v1 in mv1:
            for v2 in mv2:
                of.write(k+' '+v1+' '+v2+'\n')
of.close()
