#!/usr/bin/python
# -*- coding: utf-8 -*-

"""task4bmapper.py: Receives an xml file. Outputs uid\tpid where uid is the owner user id 
                    and pid is the parent id, only prints answers.

hadoop jar /opt/hadoop/hadoop-2.7.1/share/hadoop/tools/lib/hadoop-streaming-2.7.1.jar \
-input /data/assignments/ex2/task3/stackLarge.txt \
-output /user/$USER/data/output/cw2/s1556573_task_4bjob1.out \
-mapper task4bmapper1.py \
-file ~/exc/cw2/task4bmapper1.py \
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
            if int(post_type_id) == 2: # if post is answer
                post_id = root.get('Id','')
                owner_user_id = root.get('OwnerUserId','')
                parent_id = root.get('ParentId','')
                if owner_user_id != '':
                    print "%s%s%s" % (owner_user_id, separator, parent_id)

if __name__ == "__main__":
    main()


