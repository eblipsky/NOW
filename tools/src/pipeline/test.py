#!/usr/bin/python
from process import *

fq='iongc107257_RY1JV001'
str={"node": fq}
str1="[{\"node\": \"fq\"}]"

r = get_client()
tmp = r.get('fq_time_' + fq)
print tmp
print ""

#tmp = json.loads(tmp)
tmp = json.loads(str1)
tmp.append(str)
#print tmp[3]['node']

print ""
print json.dumps(tmp)
