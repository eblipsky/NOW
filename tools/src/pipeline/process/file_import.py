#!/usr/bin/python

from settings import *
from common import *

##################################################################
def import_files(DIR):

    #make this do bam files on a second pass!!!

    global r
    r = get_client()
    # looking for files with abc_123[_*] first part will be index
    for file in os.listdir(DIR):
        if "." not in file: continue
        filename,ext = file.split('.',1)
        # import for fq files
        if (ext == 'fq' ) or ( ext == 'bam'):
            parts = filename.split('_')
            if (len(parts) <= 1):
                continue
            fq = parts[0]+'_'+parts[1]
            print 'checking: '+file
            if r.sismember('fq',fq):
                print 'already exists, skipping'
            else:
                print 'importing ...'
                r.sadd('fq',fq)
                r.hset('finfo_'+fq,'dir',DIR)
                r.hset('finfo_'+fq,'filename',file)
                if (os.path.exists(DIR+'/'+filename+'.var')):
                    print 'adding variables from .var file ...'
                    var_file = open(DIR+'/'+filename+'.var', 'r')
                    for line in var_file:
                        if "=" in line:
                            var,val = line.strip().split('=')
                            #print '-'+var+'='val
                            r.hset('var_'+fq,var,val)
        elif (ext == 'bam'):
            pass
