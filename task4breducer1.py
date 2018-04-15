#!/usr/bin/python
# -*- coding: utf-8 -*-

"""task4breducer1.py: Receives uid\tpid where uid is the owner user id and pid is the question id 
		      Outputs uid\tpcount\tpid1,pid2,....,pidn

hadoop jar /opt/hadoop/hadoop-2.7.1/share/hadoop/tools/lib/hadoop-streaming-2.7.1.jar \
-input /data/assignments/ex2/task3/stackLarge.txt \
-output /user/$USER/data/output/cw2/s1556573_task_4bjob1.out \
-mapper task4bmapper1.py \
-file ~/exc/cw2/task4bmapper1.py \
-reducer task4breducer1.py \
-file ~/exc/cw2/task4breducer1.py \
-jobconf mapred.job.name="task4"

cat input/task4/stackLarge.txt | ./task4bmapper1.py | sort -k1,1n -k2,2n | ./task4breducer1.py
"""

import sys



def main(separator='\t'):
    prev_owner = ""
    posts = []

    for line in sys.stdin:	
        if line:
            data = line.strip().split(separator)
            if len(data) > 1:
                owner_id = data[0]
                post_id = data[1]
                if prev_owner == owner_id:
                    posts.append(post_id)
                else: # either next owner or first owner
                    if prev_owner:
                        posts_str = ", ".join(posts)
                        print "%s%s%d%s%s" % (prev_owner, separator, len(posts), separator, posts_str)
                    
                    # init values for next owner
                    posts = []
                    prev_owner = owner_id
                    posts.append(post_id)

    # dont forget last record
    if prev_owner:
        posts_str = ", ".join(posts)
        print "%s%s%d%s%s" % (prev_owner, separator, len(posts), separator, posts_str)

if __name__ == "__main__":
    main()
