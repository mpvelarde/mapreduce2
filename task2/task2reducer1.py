#!/usr/bin/python
# -*- coding: utf-8 -*-

"""task2reducer1.py: Receives t\td\ttf where t is a term in a file, d is the filename and tf is the term frequency.
		     Outputs t\td\ttf\df where t is a term in the file, d is the filename, tf is the term frequency and df is the 			     document frequency (documents where the term appears)"""

__author__      = "Maria Velarde"
__copyright__   = "Amar es compartir"

"""
hadoop jar /opt/hadoop/hadoop-2.7.1/share/hadoop/tools/lib/hadoop-streaming-2.7.1.jar \
-input /data/assignments/ex2/task1/large/ \
-output /user/$USER/data/output/cw2/s1556573_task_2job1.out \
-mapper task2mapper1.py \
-file ~/exc/cw2/task2mapper1.py \
-file ~/exc/cw2/terms.txt \
-reducer task2reducer1.py \
-file ~/exc/cw2/task2reducer1.py \
-jobconf mapred.job.name="task2"

cat input/task1/small/* | ./task2mapper1.py | sort -k1,1n -k2,2n | ./task2reducer1.py
"""

import sys

prev_term = ""
occurences = dict()

def main(separator='\t'):
    global prev_term
    global occurences
    for line in sys.stdin:	
        if line:
            data = line.strip().split(separator)
            term = data[0]
            
            if prev_term == term:	# if belongs to the same term
                if data[1] in occurences:
                    occurences[data[1]] += data[2]
                else:
                    occurences[data[1]] = data[2]
            else: 
	        # we found next term 
	        # so prev_term should be printed if exists
	        if prev_term:
	            df = len(occurences)
	            for d,tf in occurences.items():
                        print '%s%s%s%s%d%s%d' % (prev_term, separator, d, separator, int(tf), separator, int(df))
      
                # init everything for new term
	        occurences = dict()
	        prev_term = term;
 	        if data[1] in occurences:
                    occurences[data[1]] += data[2]
                else:
                    occurences[data[1]] = data[2]

    if prev_term:	# dont forget last term
        df = len(occurences)
	for d,tf in occurences.items():
            print '%s%s%s%s%d%s%d' % (prev_term, separator, d, separator, int(tf), separator, int(df))

if __name__ == "__main__":
    main()
