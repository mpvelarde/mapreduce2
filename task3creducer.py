#!/usr/bin/python
# -*- coding: utf-8 -*-

"""task3creducer.py: Receives h\tt where h is a host and t is the datetime str
		     Outputs h\tdif where dif is the difference in seconds between 1st and last visit or timestamp of only visit

hadoop jar /opt/hadoop/hadoop-2.7.1/share/hadoop/tools/lib/hadoop-streaming-2.7.1.jar \
-input /data/assignments/ex2/task2/logsLarge.txt \
-output /user/$USER/data/output/cw2/s1556573_task_3c.out \
-mapper task3cmapper.py \
-file ~/exc/cw2/task3cmapper.py \
-reducer task3creducer.py \
-file ~/exc/cw2/task3creducer.py \
-jobconf mapred.job.name="task3"

cat input/task3/logsSmall.txt | ./task3cmapper.py | sort -k1,1n -k2,2n | ./task3creducer.py
"""

import sys
import dateutil.parser
import datetime
import pytz

def main(separator='\t'):
    prev_host = ""
    min_dt = None
    max_dt = None

    for line in sys.stdin:	
        if line:
            data = line.strip().split(separator)
            if len(data) > 1:
                host = data[0]
                dt = data[1]
                timestamp = datetime.datetime.strptime(dt, "[%d/%b/%Y:%H:%M:%S -0400]")
                if prev_host == host:	# if belongs to the same host
                    # update min_dt and max_dt
	            if timestamp < min_dt:
                        min_dt = timestamp
                    if timestamp > max_dt:
                        max_dt = timestamp
                else: 
                    if prev_host: # we moved to the nex host, print time diff between min and max
                        if min_dt == max_dt: # if only one visit, print the timestamp
                            print "%s%s%s" % (prev_host, separator, min_dt.strftime("%d/%b/%Y:%H:%M:%S -0400"))
                        else:
                            timediff = max_dt - min_dt
                            diff_seconds = timediff.total_seconds()
                            print "%s%s%s" % (prev_host, separator, str(diff_seconds))
                    # init everything for new host
	            min_dt = timestamp
                    max_dt = timestamp
	            prev_host = host
    if prev_host:	# dont forget last term
        if min_dt == max_dt: # if only one visit, print the timestamp
            print "%s%s%s" % (prev_host, separator, min_dt)
        else:
            timediff = max_dt - min_dt
            diff_seconds = timediff.total_seconds()
            print "%s%s%s" % (prev_host, separator, str(diff_seconds))

if __name__ == "__main__":
    main()
