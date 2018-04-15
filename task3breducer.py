#!/usr/bin/python
# -*- coding: utf-8 -*-

"""task3breducer1.py: Receives h\tc where h where h is a host that produced a 404 error and c is count
		     Outputs h\tc top 10

hadoop jar /opt/hadoop/hadoop-2.7.1/share/hadoop/tools/lib/hadoop-streaming-2.7.1.jar \
-input /data/assignments/ex2/task2/logsLarge.txt \
-output /user/$USER/data/output/cw2/s1556573_task_3b.out \
-mapper task3bmapper.py \
-file ~/exc/cw2/task3bmapper.py \
-reducer task3breducer.py \
-file ~/exc/cw2/task3breducer.py \
-jobconf mapred.reduce.tasks=1 \
-jobconf mapred.job.name="task3"

cat input/task3/logsLarge.txt | ./task3bmapper.py | sort -k1,1n -k2,2n | ./task3breducer.py
"""

import sys

def main(separator='\t'):
    prev_host = ""
    host_count = 0
    h = []

    for line in sys.stdin:	
        if line:
            data = line.strip().split(separator)
            if len(data) > 1:
                host = data[0]
                count = data[1]
                count = int(count)
                if prev_host == host:	# if belongs to the same host
	            host_count += count
                else: 
                    if prev_host:
                        if len(h) < 10:
                            h.append((prev_host,host_count))
                        else:
                            # get min value in dict
                            h.sort(key=lambda tup: tup[1])
                            min_h = h[0]
                            if host_count > min_h[1]: # update top page
                                h[0] = (prev_host,host_count)
                    # init everything for new page
	            host_count = count
	            prev_host = host
    if prev_host:	# dont forget last term
        # get min value in dict
        h.sort(key=lambda tup: tup[1])
        min_h = h[0]
        if host_count > min_h[1]: # update top page
            h[0] = (prev_host,host_count)

    # print top 10
    h.sort(key=lambda tup: tup[1], reverse = True)
    for k, v in h:
        print "%s%s%d" % (k, separator, v)

if __name__ == "__main__":
    main()
