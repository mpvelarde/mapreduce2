#!/usr/bin/python
# -*- coding: utf-8 -*-

"""task2mapper.py: Receives t: df: {(d1, tf1),...,(dn, tfn)} where t is the term, df is the document frequency, 
		    d is the document name and tf is the term frequency per document. 
                    Outputs d\tt\ttf\df where t is a term in the file, d is the filename and tf is the term frequency.
                    and aditionally 0\td\t1 per document

hadoop jar /opt/hadoop/hadoop-2.7.1/share/hadoop/tools/lib/hadoop-streaming-2.7.1.jar \
-input /user/$USER/data/output/cw2/s1556573_task_1.out \
-output /user/$USER/data/output/cw2/s1556573_task_2.out \
-mapper task2mapper.py \
-file ~/exc/cw2/task2mapper.py \
-file ~/exc/cw2/terms.txt \
-jobconf mapred.job.name="task2"

cat output/s1556573_task_1.out/* | ./task2mapper.py
"""

import sys
import string

def main(separator='\t'):
    file_name = "local"
    terms = set()
    dnames = dict()
    for line in file('terms.txt'):
        terms.add(line[:-1])

    for line in sys.stdin:
        line = line.strip() # remove whitespaces
        if line:
            # split the line into words
            data = line.split(":")
            term = data[0]
            df = data[1]
            docs = data[2]
            docs = docs.replace("{","").replace("}","").replace("(","").replace(")","")
            docs = docs.split(",")
            docs = map(','.join, zip(docs[::2], docs[1::2]))
            for doc in docs:
                d = doc.split(",")
                d_name = d[0]
                d_name = d_name.strip()
                d_tf = d[1]
                if not d_name in dnames:
                    dnames[d_name] = 1
                if d_name == "d1.txt" and term in terms:
	            print "%s%s%s%s%s%s%s"%(d_name,separator,term,separator,d_tf.strip(),separator,df)
    # print all docs
    for k,v in dnames.items():
        print "0%s%s%s%d"%(separator,k,separator,v)

if __name__ == "__main__":
    main()

