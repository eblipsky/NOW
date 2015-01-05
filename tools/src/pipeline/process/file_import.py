#!/usr/bin/python

from settings import *
from common import *

# ToDo: Here is the process
# 1. pipeline import - make this default to the data dir
#   1. all files in the base of data dir are the ori file
#   2. on import all files get a guid - this is how we track everything about them
# 2. for each bam or fq
#   1. check for var file, if no file ask for manual variables
#   2. mkdir for file new guid
#   3. copy file into dir

# ToDo: FQ will need to be a function


##################################################################
def import_files():
    # loop through files in DATA_DIR
    # if file does not have uuid4 assiged give it one
    # check for .var file and add defined file vars
    # add to fq redis list

    global BASE_DIR
    global DATA_DIR
    global r
    r = get_client()

    BASE_DIR = r.hget('SystemSettings', 'BASE_DIR')
    DATA_DIR = BASE_DIR + "/data"
    
    found_files = False
    for file in os.listdir(DATA_DIR):
        if os.path.isfile(DATA_DIR +'/' + file):
            if file == '.gitignore': 
                continue
            if not r.hexists('imported_files', file):
                print "addng " + file + " into NOW"
                uid = uuid.uuid4()
                r.hset('imported_files', file, uid)
            
                # use the uid as the fileobject info hash
                # r.hset(uid,'fileinfo','value')

                # check for var file and add those variables

                # add to fq set so we can choose it in the workflow
                r.sadd('fq',file)
                found_files = True
            if r.hexists('imported_files', file):
                if not r.sismember('fq', file):
                    print "restoring " + file + " into NOW"
                    r.sadd('fq',file)
                    found_files = True

    if found_files:
        print "Files Imported"
    else:
        print "No new files found."
                


##################################################################
def import_files_old(DIR):

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
