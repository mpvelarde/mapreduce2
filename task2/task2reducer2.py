#!/usr/bin/python
# -*- coding: utf-8 -*-

"""task2reducer2.py: Receives d\tt\ttf\tdf where t is a term in a file, d is the filename and tf is the term frequency.
		     Outputs d\tt\ttf\tdf and aditionally 0\td\t1 per document"""

__author__      = "Maria Velarde"
__copyright__   = "Amar es compartir"

"""
hadoop jar /opt/hadoop/hadoop-2.7.1/share/hadoop/tools/lib/hadoop-streaming-2.7.1.jar \
-input /user/$USER/data/output/cw2/s1556573_task_2job1.out/ \
-output /user/$USER/data/output/cw2/s1556573_task_2job2.out \
-mapper task2mapper2.py \
-file ~/exc/cw2/task2mapper2.py \
-reducer task2reducer2.py \
-file ~/exc/cw2/task2reducer2.py \
-jobconf mapred.job.name="task2"

cat output/s1556573_task_2job1.out/* | ./task2mapper2.py | sort -k1,1n -k2,2n | ./task2reducer2.py
"""

import sys

docs = dict()

def main(separator='\t'):
    global docs
    for line in sys.stdin:	
        if line:
            data = line.strip().split(separator)
            doc = data[0]
            if not doc in docs:
                docs[doc] = 1
            print line

    # print all docs
    for k,v in docs.items():
        print "0%s%s%s%d"%(separator,k,separator,v)

if __name__ == "__main__":
    main()
