#!/usr/bin/python
# -*- coding: utf-8 -*-

"""task1mapper.py: Receives a file. Outputs t\td\ttf where t is a term in the file, d is the filename and tf is the term frequency.

hadoop jar /opt/hadoop/hadoop-2.7.1/share/hadoop/tools/lib/hadoop-streaming-2.7.1.jar \
-input /data/assignments/ex2/task1/large/ \
-output /user/$USER/data/output/cw2/s1556573_task_1.out \
-mapper task1mapper.py \
-file ~/exc/cw2/task1mapper.py \
-jobconf mapred.job.name="task1"
"""

import sys
import os, os.path
import string

def printOutput(separator,h): 
    file_name = "local"
    map_input_file = os.getenv('mapreduce_map_input_file')
    file_name = os.path.basename(map_input_file)
    for k,v in h.items():
        print '%s%s%s%s%d' % (k, separator, file_name, separator, v)


def main(separator='\t'):
    h = dict()
    N = 20

    for line in sys.stdin:
        line = line.strip() # remove whitespaces
        if line:
            # split the line into words
            words = line.split()
            for word in words:
	        if word in h:
                    h[word] += 1
                else:
                    h[word] = 1

                if len(h) > N:
                    printOutput(separator,h)
                    # flush memory
                    h = dict()

    # flush memory
    printOutput(separator,h)
    h = dict()

if __name__ == "__main__":
    main()


