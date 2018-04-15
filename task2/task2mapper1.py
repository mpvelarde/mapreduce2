#!/usr/bin/python
# -*- coding: utf-8 -*-

"""task2mapper1.py: Receives a file. Outputs t\td\ttf where t is a term in the file, d is the filename and tf is the term frequency."""

__author__      = "Maria Velarde"
__copyright__   = "Amar es compartir"

"""
hadoop jar /opt/hadoop/hadoop-2.7.1/share/hadoop/tools/lib/hadoop-streaming-2.7.1.jar \
-input /data/assignments/ex2/task1/large/ \
-output /user/$USER/data/output/cw2/s1556573_task_2job1.out \
-mapper task2mapper1.py \
-file ~/exc/cw2/task2mapper1.py \
-file ~/exc/cw2/terms.txt \
-jobconf mapred.job.name="task2"

cat input/task1/small/* | ./task2mapper1.py
"""

import sys
import string
import os, os.path

file_name = "local"
h = dict()
terms = set()
N = 100

def flushMapper(separator): # flush memory
    global h
    global file_name
    map_input_file = os.getenv('mapreduce_map_input_file')
    if map_input_file:
        file_path, file_name = os.path.split(map_input_file)
    for k,v in h.items():
        print '%s%s%s%s%d'% (k, separator, file_name, separator, v)
    del h
    h = dict()

def main(separator='\t'):
    global h
    global terms
    global N
    for line in file('terms.txt'):
        terms.add(line[:-1])

    for line in sys.stdin:
        line = line.strip() # remove whitespaces
        exclude = set(string.punctuation)
        line = ''.join(ch for ch in line if ch not in exclude) # remove punctuation
        if line:
            # split the line into words
            words = line.split()
            for word in words:
	        if word in terms:
	            if word in h:
                        h[word] += 1
                    else:
                        h[word] = 1
                    if len(h) > N:
                       flushMapper(separator)
    flushMapper(separator)

if __name__ == "__main__":
    main()

