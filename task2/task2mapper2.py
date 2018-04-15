#!/usr/bin/python
# -*- coding: utf-8 -*-

"""task2mapper2.py: Receives t\td\ttf\df where t is a term in the file, d is the filename, tf is the term frequency and df is the 			    document frequency (documents where the term appears)
		    Outputs d\tt\ttf\df"""

__author__      = "Maria Velarde"
__copyright__   = "Amar es compartir"

"""
hadoop jar /opt/hadoop/hadoop-2.7.1/share/hadoop/tools/lib/hadoop-streaming-2.7.1.jar \
-input /user/$USER/data/output/cw2/s1556573_task_2job1.out/ \
-output /user/$USER/data/output/cw2/s1556573_task_2job2.out \
-mapper task2mapper2.py \
-file ~/exc/cw2/task2mapper2.py \
-jobconf mapred.job.name="task2"

"""

import sys

def main(separator='\t'):
    for line in sys.stdin:
        if line:
            data = line.strip().split(separator)
            print '%s%s%s%s%s%s%s' % (data[1], separator, data[0], separator, data[2], separator, data[3])

if __name__ == "__main__":
    main()

