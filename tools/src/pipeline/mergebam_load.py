#!/usr/bin/python
from process import *

r = get_client()

with open(BASE_DIR+'/p.csv') as f:
    lines = f.readlines()

for line in lines:
    key,members = line.split(',')

    while (r.spop('mergebam_'+key)!=None):
        pass

    for member in members.split(';'):
        print key+":"+member
        r.sadd('mergebam_'+key,member.rstrip())
