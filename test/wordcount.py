#!/bin/python

import sys, os, glob

indir = sys.argv[1]
outfile = sys.argv[2]

wordcount={}

for filename in glob.glob(indir+'/*'):
    with open(filename, "r") as ins:
        for line in ins:
            line = line.replace('\n', '')
            words = line.split(' ')
            for word in words:
                if word == '':
                    continue
                if word not in wordcount:
                    wordcount[word] = 1
                else:
                    wordcount[word] += 1

of = open(outfile, 'w')
for k,v in wordcount.items():
    of.write(str(k) + ' ' + str(v)+'\n')
of.close()
