import re
from pyspark import SparkContext
from collections import defaultdict
import sys
import os
#sys.stdout=open("testing.txt","w")
sc = SparkContext()

def cleaning(text):
    lower_text = text.lower()
    lower_a = lower_text
    removeApos = lower_a.replace("'", "")
    removeDash = removeApos.replace("-", "")
    removePunc = re.sub(r'[^\w\s]', ' ', removeDash)
    removeUnderScore=removePunc.replace("_"," ")
    refinedEvent = removeUnderScore.strip()
    return refinedEvent

mapIn=sys.argv[1]
directory=sys.argv[2]

input=sc.textFile(mapIn).map(lambda s: s.encode("ascii", "ignore").split(",")).map(lambda s : (cleaning(s[3]),s[18]))
header=input.filter(lambda l:"page_count" in l)

inputWH= input.subtract(header)

def isNotEmpty(s,t):
    if(s and s.strip()):
        return (s,int(t))

removingNulls=inputWH.filter(lambda (a,b):isNotEmpty(a,b))


result=removingNulls.aggregateByKey((0,0),lambda U,v:(U[0]+int(v),U[1]+1),lambda U1,U2:(U1[0]+U2[0],U1[1]+U2[1]))

result=result.sortByKey(True)
res=result.map(lambda(x,(y,z)):(x,z,float(y)/z))
rescol=res.collect()

if not os.path.exists(directory):
    os.makedirs(directory)
f= open(os.path.join(directory,'Reshma_Bhatia_task2.txt'),'w');
for(key,count,value) in rescol:
    f.write("%s\t%i\t%.3f" % (key,count,value)+"\n")
f.close()



