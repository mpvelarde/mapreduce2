#!/usr/bin/python
# -*- coding: utf-8 -*-

"""task2reducer3.py: Receives 0\td\t1 per document and d\tt\ttf\tdf where t is a term in a file, 
                     d is the filename and tf is the term frequency.
		     Outputs t, d = tf-idf """

__author__      = "Maria Velarde"
__copyright__   = "Amar es compartir"

"""
hadoop jar /opt/hadoop/hadoop-2.7.1/share/hadoop/tools/lib/hadoop-streaming-2.7.1.jar \
-input /user/$USER/data/output/cw2/s1556573_task_2job2.out/ \
-output /user/$USER/data/output/cw2/s1556573_task_2.out \
-mapper task2mapper3.py \
-file ~/exc/cw2/task2mapper3.py \
-reducer task2reducer3.py \
-file ~/exc/cw2/task2reducer3.py \
-jobconf mapred.reduce.tasks=1 \
-jobconf mapred.job.name="task2"

cat output/s1556573_task_2job2.out/* | ./task2mapper3.py | sort -k1,1n -k2,2n | ./task2reducer3.py
"""

import sys
import math

total_docs = 0

def main(separator='\t'):
    global total_docs
    for line in sys.stdin:	
        if line:
            data = line.strip().split(separator)
            if len(data) > 3:
                tfidf = getTfidf(int(data[2]), total_docs, int(data[3]))
                print "%s, %s = %s"%(data[1], data[0], tfidf) 
            else:
                total_docs += 1
                

def getTfidf(tf, N, df):
    idf = math.log10(N/(1+df))
    return tf*idf

if __name__ == "__main__":
    main()
