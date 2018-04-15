#!/usr/bin/python
# -*- coding: utf-8 -*-

"""task3bmapper.py: Receives a file. Outputs h\tc where h is a host that produced a 404 error and c is count.
                    Implement in-mapper combinator to avoid sending too much uotput to the network

hadoop jar /opt/hadoop/hadoop-2.7.1/share/hadoop/tools/lib/hadoop-streaming-2.7.1.jar \
-input /data/assignments/ex2/task2/logsLarge.txt \
-output /user/$USER/data/output/cw2/s1556573_task_3b.out \
-mapper task3bmapper.py \
-file ~/exc/cw2/task3bmapper.py \
-jobconf mapred.job.name="task3"

cat input/task3/logsLarge.txt | ./task3bmapper.py
"""

import sys
import os, os.path
import string

def printOutput(separator, h):
    for k,v in h.items():
        print '%s%s%d' % (k, separator, v)
    
def main(separator='\t'):
    h = dict()
    N = 20
    for line in sys.stdin:
        line = line.strip() # remove whitespaces
        if line:
            data = line.split('- -')
            host = data[0]
            record = data[1].strip().split('"')
            if len(record) > 1:
                replybytes = record[2]
                parts = replybytes.strip().split(" ")
                if len(parts) > 1:
                    reply = parts[0]
                    if reply == "404":
                        if host in h:
                            h[host] += 1
                        else:
                            h[host] = 1
                    if len(h) > N:
                        printOutput(separator, h)
                        del h
                        h = dict()
    printOutput(separator, h)
    del h
    h = dict()

if __name__ == "__main__":
    main()


