#!/usr/bin/python
# -*- coding: utf-8 -*-

"""task2mapper3.py: Prints line"""

__author__      = "Maria Velarde"
__copyright__   = "Amar es compartir"

"""
hadoop jar /opt/hadoop/hadoop-2.7.1/share/hadoop/tools/lib/hadoop-streaming-2.7.1.jar \
-input /user/$USER/data/output/cw2/s1556573_task_2job2.out/ \
-output /user/$USER/data/output/cw2/s1556573_task_2.out \
-mapper task2mapper3.py \
-file ~/exc/cw2/task2mapper3.py \
-jobconf mapred.job.name="task2"

"""

import sys

def main(separator='\t'):
    for line in sys.stdin:
        if line:
            print '%s' % (line)

if __name__ == "__main__":
    main()

