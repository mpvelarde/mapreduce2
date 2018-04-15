#!/usr/bin/python
# -*- coding: utf-8 -*-

"""task4areducer.py: Receives qid\tvcount where qid is the question id and vcount is the views count attribute
		     Outputs qid,\tvcount top 10


hadoop jar /opt/hadoop/hadoop-2.7.1/share/hadoop/tools/lib/hadoop-streaming-2.7.1.jar \
-input /data/assignments/ex2/task3/stackLarge.txt \
-output /user/$USER/data/output/cw2/s1556573_task_4a.out \
-mapper task4amapper.py \
-file ~/exc/cw2/task4amapper.py \
-reducer task4areducer.py \
-file ~/exc/cw2/task4areducer.py \
-jobconf mapred.reduce.tasks=1 \
-jobconf mapred.job.name="task4"

cat input/task4/stackLarge.txt | ./task4amapper.py | sort -k1,1n -k2,2n | ./task4areducer.py
"""

import sys

def main(separator='\t'):
    h = []
    for line in sys.stdin:	
        if line:
            data = line.strip().split(separator)
            if len(data) > 1:
                post_id = data[0]
                view_count = data[1]
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
        print "%s,%s%s" % (k, separator, v)

if __name__ == "__main__":
    main()
