#!/usr/bin/python
# -*- coding: utf-8 -*-

"""task4bmapper2.py: Receives uid\tpcount\tpid1,pid2,....,pidn where uid is the user id, pcount is the 
                    number of answers per user user and pid1,pid2,...,pidn are the ids of the answers 
                    Outputs uid\tpcount\tpid1,pid2,....,pidn for the user with the biggest pcount.

hadoop jar /opt/hadoop/hadoop-2.7.1/share/hadoop/tools/lib/hadoop-streaming-2.7.1.jar \
-input /user/$USER/data/output/cw2/s1556573_task_4bjob1.out \
-output /user/$USER/data/output/cw2/s1556573_task_4b.out \
-mapper task4bmapper2.py \
-file ~/exc/cw2/task4bmapper2.py \
-jobconf mapred.job.name="task4"

cat output/s1556573_task_4bjob1.out/* | ./task4bmapper2.py
"""

import sys


def main(separator='\t'):
    top_owner = ""
    top_pcount = 0
    top_posts = ""

    for line in sys.stdin:
        line = line.strip() # remove whitespaces
        if line:
            data = line.split(separator)
            if len(data) > 2:
                owner_id = data[0]
                pcount = int(data[1])
                plist = data[2]
                
                if pcount > top_pcount: # get top pcount
                    top_owner = owner_id
                    top_pcount = pcount
                    top_posts = plist
    print "%s%s%s%s%s" % (top_owner, separator, top_pcount, separator, top_posts)                

if __name__ == "__main__":
    main()


