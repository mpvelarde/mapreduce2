#!/usr/bin/python
# -*- coding: utf-8 -*-

"""task4cmapper1.py: Receives an xml file. Outputs aid\tptype for posts or pid\tptype\tuid for answers
                    where pid is the post id, ptype is the post type, uid is the owner user id 
                    and aid is the accepted answer id.
                    Outputs  qid\tptype for questions or aid\tptype\tuid for answers

hadoop jar /opt/hadoop/hadoop-2.7.1/share/hadoop/tools/lib/hadoop-streaming-2.7.1.jar \
-input /data/assignments/ex2/task3/stackLarge.txt \
-output /user/$USER/data/output/cw2/s1556573_task_4cjob1.out \
-mapper task4cmapper1.py \
-file ~/exc/cw2/task4cmapper1.py \
-jobconf mapred.job.name="task4"

cat input/task4/stackSmall.txt | ./task4bmapper1.py
"""

import sys
import xml.etree.ElementTree as ET

def main(separator='\t'):
    for line in sys.stdin:
        line = line.strip() # remove whitespaces
        if line:
            root = ET.fromstring(line)
            post_type_id = root.get('PostTypeId','')
            if int(post_type_id) == 1: # if post is question
                post_id = root.get('Id','')
                accepted_answer_id = root.get('AcceptedAnswerId','')
                if accepted_answer_id != '':
                    print "%s%s%s" % (accepted_answer_id, separator, post_type_id)
            elif int(post_type_id) == 2: # if post is answer
                post_id = root.get('Id','')
                owner_user_id = root.get('OwnerUserId','')
                if owner_user_id != '' and post_id != '':
                    print "%s%s%s%s%s" % (post_id, separator, post_type_id, separator, owner_user_id)

if __name__ == "__main__":
    main()


