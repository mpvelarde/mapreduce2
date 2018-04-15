#!/usr/bin/python
# -*- coding: utf-8 -*-

"""task3bmapper1.py: Receives a file. Outputs h\tt where h is a host and t is the datetime str.

hadoop jar /opt/hadoop/hadoop-2.7.1/share/hadoop/tools/lib/hadoop-streaming-2.7.1.jar \
-input /data/assignments/ex2/task2/logsLarge.txt \
-output /user/$USER/data/output/cw2/s1556573_task_3c.out \
-mapper task3cmapper.py \
-file ~/exc/cw2/task3cmapper.py \
-jobconf mapred.job.name="task3"

cat input/task3/logsSmall.txt | ./task3cmapper.py
"""

import sys
import string

def main(separator='\t'):
    for line in sys.stdin:
        line = line.strip() # remove whitespaces
        if line:
            data = line.split('- -')
            if len(data) > 1:
                host = data[0]
                record = data[1].strip().split('"')
                if len(record) > 1:
                    timestamp_str = record[0]
		    timestamp_str = timestamp_str.replace('"', '').strip()
                    print '%s%s%s' % (host, separator, timestamp_str)

if __name__ == "__main__":
    main()


