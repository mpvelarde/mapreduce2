#!/usr/bin/python
# -*- coding: utf-8 -*-

"""task4creducer2.py: Receives uid\tpid where uid is the user and tpid is the answer id
		      Outputs uid\tpcount\tpid1,pid2,...,pidn

hadoop jar /opt/hadoop/hadoop-2.7.1/share/hadoop/tools/lib/hadoop-streaming-2.7.1.jar \
-input /user/$USER/data/output/cw2/s1556573_task_4cjob1.out \
-output /user/$USER/data/output/cw2/s1556573_task_4cjob2.out \
-mapper cat \
-reducer task4creducer2.py \
-file ~/exc/cw2/task4creducer2.py \
-jobconf mapred.job.name="task4"

cat input/task4/stackSmall.txt | ./task4cmapper1.py | sort -k1,1n -k2,2n | ./task4creducer1.py
"""

import sys

def main(separator='\t'):
    posts = []
    prev_user_id = ""

    for line in sys.stdin:	
        if line:
            data = line.strip().split(separator)
            if len(data) > 1:
                user_id = data[0]
                post_id = data[1]
                if prev_user_id == user_id:
                    posts.append(post_id)
                else: # either next post or first post
                    if prev_user_id:
                        print "%s%s%d%s%s" % (prev_user_id, separator, len(posts), separator, ",".join(posts))
                    
                    # init values for next post
                    prev_user_id = user_id
                    posts = []
                    posts.append(post_id)

    # dont forget last record
    if prev_user_id:
        print "%s%s%d%s%s" % (prev_user_id, separator, len(posts), separator, ",".join(posts))

if __name__ == "__main__":
    main()
