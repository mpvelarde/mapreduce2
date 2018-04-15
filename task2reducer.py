#!/usr/bin/python
# -*- coding: utf-8 -*-

"""task2reducer.py: Receives 0\td\t1 per document and d\tt\ttf\tdf where t is a term in a file, 
                     d is the filename and tf is the term frequency.
		     Outputs t, d = tf-idf

hadoop jar /opt/hadoop/hadoop-2.7.1/share/hadoop/tools/lib/hadoop-streaming-2.7.1.jar \
-input /user/$USER/data/output/cw2/s1556573_task_1.out \
-output /user/$USER/data/output/cw2/s1556573_task_2.out \
-mapper task2mapper.py \
-file ~/exc/cw2/task2mapper.py \
-reducer task2reducer.py \
-file ~/exc/cw2/task2reducer.py \
-jobconf mapred.reduce.tasks=1 \
-file ~/exc/cw2/terms.txt \
-jobconf mapred.job.name="task2"

cat output/s1556573_task_1.out/* | ./task2mapper.py | sort -k1,1n -k2,2n | ./task2reducer.py
"""

import sys
import math

def main(separator='\t'):
    docs = dict()
    for line in sys.stdin:	
        if line:
            data = line.strip().split(separator)
            if len(data) > 3:
                #print "N: %d, tf: %d, df: %d"%(int(len(docs)),int(data[2]), int(data[3]))
                tfidf = getTfidf(int(data[2]), int(len(docs)), int(data[3]))
                print "%s, %s = %s"%(data[1], data[0], str(tfidf)) 
            elif len(data) > 2:
                if int(data[0]) == 0:
                    if not data[1] in docs:
                        docs[data[1]] = 1
                

def getTfidf(tf, N, df):
    idf = math.log10(N/float(1+df))
    return tf*float(idf)

if __name__ == "__main__":
    main()
