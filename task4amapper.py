#!/usr/bin/python
# -*- coding: utf-8 -*-

"""task4amapper.py: Receives an xml file. Outputs top 10 viewed questions as qid\tvcount where qid is the question id 
                    and vcount is the views count attribute.

hadoop jar /opt/hadoop/hadoop-2.7.1/share/hadoop/tools/lib/hadoop-streaming-2.7.1.jar \
-input /data/assignments/ex2/task3/stackLarge.txt \
-output /user/$USER/data/output/cw2/s1556573_task_4a.out \
-mapper task4amapper.py \
-file ~/exc/cw2/task4amapper.py \
-jobconf mapred.job.name="task4"

cat input/task4/stackLarge.txt | ./task4amapper.py
"""

import sys
import xml.etree.ElementTree as ET

def main(separator='\t'):
    h = []
    for line in sys.stdin:
        line = line.strip() # remove whitespaces
        if line:
            root = ET.fromstring(line)
            post_type_id = root.get('PostTypeId')
            if int(post_type_id) == 1: # if post is question
                post_id = root.get('Id')
                view_count = root.get('ViewCount')
                if len(h) < 10:
                    h.append((post_id, int(view_count)))
                else:
                    # get min value in dict
                    h.sort(key=lambda tup: int(tup[1]))
                    min_h = h[0]
                    if int(view_count) > int(min_h[1]): # update top page
                        h[0] = (post_id, view_count)

    # print top 10
    h.sort(key=lambda tup: int(tup[1]), reverse = True)
    for k, v in h:
        print "%s%s%s" % (k, separator, v)
    h = []
                    

if __name__ == "__main__":
    main()


