#!/usr/bin/python
# -*- coding: utf-8 -*-

"""task3amapper1.py: Receives a file. Outputs p\tc where p is a page in the server and c is count.
                    Implement in-mapper combinator to avoid sending too much uotput to the network

hadoop jar /opt/hadoop/hadoop-2.7.1/share/hadoop/tools/lib/hadoop-streaming-2.7.1.jar \
-input /data/assignments/ex2/task2/logsLarge.txt \
-output /user/$USER/data/output/cw2/s1556573_task_3ajob1.out \
-mapper task3amapper1.py \
-file ~/exc/cw2/task3amapper1.py \
-jobconf mapred.job.name="task3"
"""

import sys
import os, os.path
import string

def printOutput(separator, h): # flush memory
    for k,v in h.items():
        print '%s%s%d' % (k, separator, v)

def main(separator='\t'):
    h = dict()
    N = 20
    for line in sys.stdin:
        line = line.strip() # remove whitespaces
        if line:
            data = line.split('- -')
            record = data[1].strip().split('"')
            if len(record) > 1:
                request = record[1]
                parts = request.strip().split(" ")
                if len(parts) > 1:
                    page = parts[1]
                    if page in h:
                        h[page] += 1
                    else:
                        h[page] = 1
                    if len(h) > N:
                        printOutput(separator,h)
                        # flush memory
                        h = dict()
    printOutput(separator,h)
    # flush memory
    h = dict()

if __name__ == "__main__":
    main()


