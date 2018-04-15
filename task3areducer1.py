#!/usr/bin/python
# -*- coding: utf-8 -*-

"""task3areducer1.py: Receives p\tc where p is a page in the server and c is count
		     Outputs p\tc top 1

hadoop jar /opt/hadoop/hadoop-2.7.1/share/hadoop/tools/lib/hadoop-streaming-2.7.1.jar \
-input /data/assignments/ex2/task2/logsLarge.txt \
-output /user/$USER/data/output/cw2/s1556573_task_3ajob1.out \
-mapper task3amapper1.py \
-file ~/exc/cw2/task3amapper1.py \
-reducer task3areducer1.py \
-file ~/exc/cw2/task3areducer1.py \
-jobconf mapred.job.name="task3"
"""

import sys

def main(separator='\t'):
    prev_page = ""
    page_count = 0
    top_page = ""
    top_count = 0

    for line in sys.stdin:	
        if line:
            data = line.strip().split(separator)
            if len(data) > 1:
                page = data[0]
                count = data[1]
                count = int(count)
                if prev_page == page:	# if belongs to the same page
	            page_count += count
                else: 
                    if prev_page:
                        if page_count > top_count: # update top page
                            top_count = page_count
                            top_page = prev_page
                    # init everything for new page
	            page_count = count
	            prev_page = page
    if prev_page:	# dont forget last term
        if page_count > top_count: # update top page
            top_count = page_count
            top_page = prev_page
    print "%s%s%d" % (top_page, separator, top_count)

if __name__ == "__main__":
    main()
