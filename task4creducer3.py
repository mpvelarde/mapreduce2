#!/usr/bin/python
# -*- coding: utf-8 -*-

"""task4creducer3.py: Receives uid\tpcount\tpid1,pid2,...,pidn 
                      Outputs the same, but only for user with highest pcount

hadoop jar /opt/hadoop/hadoop-2.7.1/share/hadoop/tools/lib/hadoop-streaming-2.7.1.jar \
-input /user/$USER/data/output/cw2/s1556573_task_4cjob2.out \
-output /user/$USER/data/output/cw2/s1556573_task_4c.out \
-mapper cat \
-reducer task4creducer3.py \
-file ~/exc/cw2/task4creducer3.py \
-jobconf mapred.reduce.tasks=1 \
-jobconf mapred.job.name="task4"

cat input/task4/stackSmall.txt | ./task4cmapper1.py | sort -k1,1n -k2,2n | ./task4creducer1.py
"""

import sys

def main(separator='\t'):
    top_user = ""
    top_count = 0
    top_posts = ""

    for line in sys.stdin:	
        if line:
            data = line.strip().split(separator)
            if len(data) > 1:
                user_id = data[0]
                pcount = data[1]
                posts = data[2]
                if int(pcount) > top_count:
                    top_user = user_id
                    top_count = int(pcount)
                    top_posts = posts

    print "%s -> %d, %s" % (top_user, int(top_count), top_posts)

if __name__ == "__main__":
    main()
