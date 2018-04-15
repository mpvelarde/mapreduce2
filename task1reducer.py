#!/usr/bin/python
# -*- coding: utf-8 -*-

"""task1reducer.py: Receives t\td\ttf where t is a term in a file, d is the filename and tf is the term frequency.
		    Outputs t: df: {(d1, tf1),...,(dn, tfn)} where t is the term, df is the document frequency, 
		    d is the document name and tf is the term frequency per document


hadoop jar /opt/hadoop/hadoop-2.7.1/share/hadoop/tools/lib/hadoop-streaming-2.7.1.jar \
-input /data/assignments/ex2/task1/large/ \
-output /user/$USER/data/output/cw2/s1556573_task_1.out \
-mapper task1mapper.py \
-file ~/exc/cw2/task1mapper.py \
-reducer task1reducer.py \
-file ~/exc/cw2/task1reducer.py \
-jobconf mapred.job.name="task1"
"""

import sys

def main(separator='\t'):
    prev_term = ""
    occurences = dict()

    for line in sys.stdin:	
        if line:
            data = line.strip().split(separator)
            term = data[0]
            d = data[1]
            tf = int(data[2])
            
            if prev_term == term:	# if belongs to the same term
                if d in occurences:
                    occurences[d] += tf
                else:
                    occurences[d] = tf
            else: 
	        # we found next term 
	        # so prev_term should be printed if exists
	        if prev_term:
                    occurences_str = ','.join('(%s,%s)'%(k,v) for k,v in occurences.items())
	            print "%s: %d: {%s}" % (prev_term, len(occurences), occurences_str)
      
	        # init everything for new term
	        occurences = dict()
	        prev_term = term;
	        if d in occurences:
                    occurences[d] += tf
                else:
                    occurences[d] = tf

    if prev_term:	# dont forget last term
        occurences_str = ','.join('(%s,%s)'%(k,v) for k,v in occurences.items())
	print "%s: %d: {%s}" % (prev_term, len(occurences), occurences_str)

if __name__ == "__main__":
    main()













