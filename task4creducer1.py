#!/usr/bin/python
# -*- coding: utf-8 -*-

"""task4creducer1.py: Receives  qid\tptype for questions or aid\tptype\tuid for answers
                      where pid is the post id, ptype is the post type, uid is the owner user id 
                      and aid is the accepted answer id
		      Outputs uid\tpid\t1 (only for accepted answers)

hadoop jar /opt/hadoop/hadoop-2.7.1/share/hadoop/tools/lib/hadoop-streaming-2.7.1.jar \
-input /data/assignments/ex2/task3/stackLarge.txt \
-output /user/$USER/data/output/cw2/s1556573_task_4cjob1.out \
-mapper task4cmapper1.py \
-file ~/exc/cw2/task4cmapper1.py \
-reducer task4creducer1.py \
-file ~/exc/cw2/task4creducer1.py \
-jobconf mapred.job.name="task4"

cat input/task4/stackSmall.txt | ./task4cmapper1.py | sort -k1,1n -k2,2n | ./task4creducer1.py
"""

import sys

def main(separator='\t'):
    prev_id = ""
    owner_user_id = ""
    accepted = False

    for line in sys.stdin:	
        if line:
            data = line.strip().split(separator)
            if len(data) > 1:
                post_id = data[0]
                post_type_id = data[1]
                if prev_id == post_id:
                    if int(post_type_id) == 2:
                        owner_user_id = data[2]
                    elif int(post_type_id) == 1:
                        accepted = True
                else: # either next post or first post
                    if prev_id and accepted and owner_user_id:
                        print "%s%s%s" % (owner_user_id, separator, prev_id)
                    
                    # init values for next post
                    prev_id = post_id
                    owner_user_id = ""
                    accepted = ""
                    if int(post_type_id) == 2:
                        owner_user_id = data[2]
                    elif int(post_type_id) == 1:
                        accepted = True

    # dont forget last record
    if prev_id and accepted and owner_user_id:
       print "%s%s%s" % (owner_user_id, separator, prev_id)

if __name__ == "__main__":
    main()
