#!/usr/bin/python

from settings import *

## CouchDB Classes ##############################################

class Log(Document):
    queue = StringProperty()
    node = StringProperty()
    start = StringProperty()
    end = StringProperty()
    total = StringProperty()
    fq = StringProperty()

class Pipeline(Document):
    name = StringProperty()

class Queue(Document):
    error = StringProperty()
    validchk = StringProperty(default="check_default")
    file_cnt = StringProperty(default="MAX_CPU-1")
    active = StringProperty(default="false")
    desc = StringProperty()
    pipeline = StringProperty()
    name = StringProperty()
    cmd = StringProperty()
    next = StringProperty()
    cmd_type = StringProperty(default="local")

## Common functions ##############################################

##################################################################
def listdir_fullpath(d):
    return [os.path.join(d, f) for f in os.listdir(d)]

##################################################################
def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        return False

##################################################################
def in_batch(fq):
    r = get_client()
    for batch in r.smembers('batches'):
        for f in r.lrange('batch_'+batch+'_files', 0, -1):
            if f == fq:
                return batch

            #check for demux files
            if "." in fq:
                parts = fq.split('.')
                if parts[0] == f:
                    if is_number(parts[1]):
                        return batch

    return "NoBatch"

###########################################################
def cmd_parse(pipeline, cmd, fq):
    r = get_client()
    cmd = str(cmd)

    # todo: this is a hack.. make it better
    intervals = eval(r.hget('gvars', 'intervals'))
    while '%intervals%' in cmd:
        spos = cmd.find('%intervals%')
        epos = spos + len('%intervals%')
        if '.' in fq:
            sub = int(fq.split('.')[-1])
            cmd = cmd[:spos] + intervals[sub-1] + cmd[epos:]
        else:
            cmd = cmd[:spos] + '' + cmd[epos:]

    # replace file specific vars
    vars = r.hkeys('var_'+fq)
    for var in vars:
        while '%fq['+var+']%' in cmd:
            spos = cmd.find('%fq['+var+']%')
            epos = spos + len(var) + 6
            cmd = cmd[:spos] + str(r.hget('var_'+fq, var)) + cmd[epos:]

    # replace all %fq% with DATA_DIR+fq and make dir if it doesnt exist
    while '%fq%' in cmd:
        spos = cmd.find('%fq%')
        epos = spos + len('%fq%')
        if not os.path.exists(DATA_DIR + '/' + fq):
            os.makedirs(DATA_DIR + '/' + fq)

        # lets move all the files related to fq into this dir now
        for file in os.listdir(DATA_DIR):
            if os.path.isfile(DATA_DIR+"/"+file):
                # need to make sure startwith does the whole match to the next dot
                if file.startswith(fq+".") or file.startswith(fq+"_1.") or file.startswith(fq+"_2."):
                    os.rename(DATA_DIR + '/' + file, DATA_DIR + '/' + fq + '/' + file)

        cmd = cmd[:spos] + DATA_DIR + '/' + fq + '/' + fq + cmd[epos:]

    # replace globals
    vars = r.hkeys('gvars')
    for var in vars:
        while '%'+var+'%' in cmd:
            spos = cmd.find('%'+var+'%')
            epos = spos + len(var) + 2
            cmd = cmd[:spos] + str(r.hget('gvars', var)) + cmd[epos:]

    # pipeline vars
    gvars = [['REF_HUMAN_GENOME', 'ref_genome'], ['REF_VCF', 'ref_vcf'], ['REF_BED', 'ref_bed'], ['BAIT_BED', 'ref_bait'], ['TARGET_BED', 'ref_target']]
    for var,rname in gvars:
        while '%'+var+'%' in cmd:
            spos = cmd.find('%'+var+'%')
            epos = spos + len(var) + 2
            cmd = cmd[:spos] + eval('REF_DIR') + '/' + str(r.hget(pipeline, rname)) + cmd[epos:]

    # try to eval whats left
    while '%' in cmd:
        spos = cmd.find('%')
        epos = -1
        for i in range(spos+1, len(cmd)):
            if cmd[i] == '%':
                epos = i
                break
        cmd = cmd[:spos] + str(eval(cmd[spos+1:epos])) + cmd[epos+1:]

    # eval function calls
    while '~' in cmd:
        spos = cmd.find('~')
        epos = -1
        for i in range(spos+1, len(cmd)):
            if cmd[i] == '~':
                epos = i
                break
        cmd = cmd[:spos] + str(eval(cmd[spos+1:epos])) + cmd[epos+1:]

    return cmd

##################################################################
def server_start():
    cmd = 'redis-server --dir ' + DATA_DIR
    server_process.append(Popen(cmd, shell=True))
    time.sleep(.5)
    #server_process[0].poll()
    #server_process[0].returncode    

##################################################################
def server_stop():
    r = get_client()
    r.execute_command('shutdown save')
    #server_process[0].kill()

##################################################################
def get_client():
    global r
    try:
        if r is None:
            r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
        return r
    except:
        return None

##################################################################
def node_start_all():
    if HAS_NODES:
        for i in range(len(NODE_IDS)):
            node_start(NODE_IDS[i])
    else:
        node_start(0)

##################################################################
def isNodeRunning(n):
    node = NODE_BASE_NAME + str(n)
##################################################################
def node_start(n):
    if HAS_NODES:

        if n not in NODE_IDS:
            print 'node ' + str(n) + ' not in NODE_IDS'
            exit(1)
        command = 'ssh -f ' + NODE_BASE_NAME + str(n) + ' \'cd ' + BASE_DIR + ';. tools/env_setup.sh; (python ' + SCRIPT_CLI + ') >> ' + LOG_DIR + '/process_' + str(n) + '.log 2>&1 &\''
        print 'starting node ' + str(n)

        Popen(command, shell=True)
        time.sleep(.5)

    else:
  
        # not using nodes so we should just launch the process via localhost
        print 'starting localhost as node'
        command = 'cd ' + BASE_DIR + ';. tools/env_setup.sh; nohup python ' + SCRIPT_CLI + ' >> ' + LOG_DIR + '/process_' + str(n) + '.log 2>&1 &'
        Popen(command, shell=True)

##################################################################
def node_stop_all():
    if HAS_NODES:
        for i in range(len(NODE_IDS)):
            node_stop(NODE_IDS[i])
    else:
        node_stop(0)

##################################################################
def email(body, subject, toAddress):
    email_cmd = 'echo "'+body+'" | mail -s "'+subject+'" '+toAddress
    os.system(email_cmd)

# possibly send process log tail -100 with email? following is untested with tail of the log
# ( echo "'+body+'"; uuencode ( tail -100 process_1.log ) uue_process_1.log ) | mail -s "'+subject+'" '+toAddress

##################################################################
def node_stop(n):

    global PASS
    r = get_client()

    if HAS_NODES:

        if PASS == "":
            PASS = getpass.getpass()

        if n not in NODE_IDS:
            print 'node ' + str(n) + 'not in NODE_IDS'
            exit(1)

        command2 = 'ssh -t ' + NODE_BASE_NAME + str(n) + ' \'echo ' + PASS + ' | sudo -S pkill -2 python\' >/dev/null 2>&1'
        command9 = 'ssh -t ' + NODE_BASE_NAME + str(n) + ' \'echo ' + PASS + ' | sudo -S pkill -9 python\' >/dev/null 2>&1'

        print "killing " + NODE_BASE_NAME + str(n)
        Popen(command2, shell=True)
        time.sleep(.5)
        Popen(command9, shell=True)
        time.sleep(.5)

    else:

        command2 = "ps -ef | grep python | grep process-cli.py | awk '{print $2}' | xargs kill -2 >/dev/null 2>&1"
        command9 = "ps -ef | grep python | grep process-cli.py | awk '{print $2}' | xargs kill -9 >/dev/null 2>&1"
        print "killing " + NODE_BASE_NAME
        Popen(command2, shell=True)
        time.sleep(.5)
        Popen(command9, shell=True)
        time.sleep(.5)

    r.srem('nodes', NODE_BASE_NAME + str(n))
    r.ltrim(NODE_BASE_NAME + str(n)+'_files', 1, 0)
    r.hdel(NODE_BASE_NAME + str(n), 'ver')
    r.hdel(NODE_BASE_NAME + str(n), 'stage')
    r.hdel(NODE_BASE_NAME + str(n), 'start')
    r.hdel(NODE_BASE_NAME + str(n), 'pipeline')


##################################################################
def set_cmd(cmd=[]):
    r = get_client()
    r.set('cmd_'+HOSTNAME, cmd)

##################################################################
def set_batch_info(fq, host, queue, pipeline, status):
    r = get_client()

    #todo: check if there are .x demux files and update them too

    r.hset('finfo_'+fq, 'queue', queue)
    r.hset('finfo_'+fq, 'pipeline', pipeline)
    r.hset('finfo_'+fq, 'status', status)
    r.hset('finfo_'+fq, 'node', host)

##################################################################
def set_node_info(stage='?', start=None):
    r = get_client()
    r.sadd('nodes', HOSTNAME)
    r.hset(HOSTNAME, 'user', USERNAME)
    r.hset(HOSTNAME, 'stage', stage)
    if start is not None:
        r.hset(HOSTNAME, 'start', start.strftime("%H:%M:%S"))

##################################################################
def get_file_sizes():
    r = get_client()
    for fq in r.smembers('fq'):
        s = os.stat(DATA_DIR + '/' + fq + '_1.fq').st_size
        s += os.stat(DATA_DIR + '/' + fq + '_2.fq').st_size
        r.set('fq_size_' + fq, str(round((s*9.31322574615E-10), 2)) + ' GB')

##################################################################
def review_queues():
    r = get_client()

    # check data folder for new files and add them
    for fq in os.listdir(DATA_DIR):
        fname, ext = os.path.splitext(fq)
        if ext != '.fq':
            continue
        if r.sismember('fq', fname[:-2]) is False:
            r.set('fq_time_' + fname[:-2], 'enqueue:' + datetime.now().strftime("%Y%m%d %H:%M:%S"))
            r.sadd('fq', fname[:-2])
            s = os.stat(DATA_DIR + '/' + fq).st_size
            est = str(round((((s*9.31322574615E-10)/0.0533)/60), 2)) + ' hours'
            r.set('fq_time_est_' + fname[:-2], est)
            #r.rpush('fq->sai', fname[:-2])

    get_file_sizes() 

##################################################################
def start_file(fq, pipeline):
    r.rpush(pipeline+'_queue_start', fq)

##################################################################
def cleanup():
    r.srem('nodes', HOSTNAME)
    r.ltrim(HOSTNAME+'_files', 1, 0)
    r.hdel(HOSTNAME, 'ver')
    r.hdel(HOSTNAME, 'stage')
    r.hdel(HOSTNAME, 'start')
    r.hdel(HOSTNAME, 'pipeline')

##################################################################
def set_file_info(fq, logfilename, stage, cmd, start, end, err=0):

    if stage is None:
        stage == "None"
    if cmd is None:
        cmd = "None"

    server = Server(uri=COUCHDB_HOST)
    db = server.get_or_create_db(COUCHDB_DB)
    Log.set_db(db)
    logEntry = None

    if type(err) == str:
        logEntry = Log(stage=stage, cmd=cmd, node=HOSTNAME, start=start.strftime(DATE_FMT), end=end.strftime(DATE_FMT), total=err, fq=fq)
    else:
        if err != 0:
            logEntry = Log(stage=stage, cmd=cmd, node=HOSTNAME, start=start.strftime(DATE_FMT), end=end.strftime(DATE_FMT), total="!!ERROR!!", fq=fq)
        else:
            h, remain = divmod((end-start).seconds, 3600)
            m, s = divmod(remain, 60)
            t = str(h)+':'+str(m)+':'+str(s)
            logEntry = Log(stage=stage, cmd=cmd, node=HOSTNAME, start=start.strftime(DATE_FMT), end=end.strftime(DATE_FMT), total=t, fq=fq)

    logEntry.save()
    if logfilename:
        with open(logfilename, "r") as myfile:
            logEntry.put_attachment(myfile, "logfile")

##################################################################
def import_demux(stage, fq):

    # todo: need to get the actual file object working right

    r = get_client()

    for file in glob.glob(DATA_DIR+"/"+fq+".*"):

        #ignore files
        if os.path.isfile(os.path.join(DATA_DIR, file)):
            continue

        #add new fq record
        new_fq = file.replace(DATA_DIR+"/", "")
        r.sadd('fq', new_fq)

        #copy file vars
        keys = r.hkeys('var_'+fq)
        for key in keys:
            r.hset('var_'+new_fq, key, r.hget('var_'+fq, key))

        #push file
        r.rpush(stage, new_fq)

    r.srem('fq', fq)

##################################################################
def import_mux(stage, fq):

    r = get_client()

    # this is a wee bit dangerous, do a double check for in-process files???

    base_fq = fq.split('.')[0]
    all_fqs = r.smembers('fq')
    fqs = [i for i in all_fqs if i.startswith(base_fq)]
    
    if len(fqs) == 1:
        # this is the last sub fq so push ori fq back
        # otherwise let it fall off
        r.rpush(stage, base_fq)

##################################################################
def check_default(fq=None):
    return True

##################################################################
def check_needs_base_recal(fq=None):

    cmd = "head -4000 "+fq+"_1.fq | perl -ne 'print if ($. % 4 == 0)'.'\\n' | egrep '[a-z]' | wc"

    p = Popen(cmd, shell=True, stdout=PIPE)
    out, err = p.communicate()

    if float(out) > 100:
        return True
    else:
        return False

##################################################################
def check_mergebam(fq=None):
    r = get_client()

    # check if merge_fq has smembers    
    mfiles = r.smembers('mergebam_'+fq)

    if len(mfiles) > 0:
        # check if smemeber exist
        for f in mfiles:
            if os.path.isfile(DATA_DIR+"/"+f+".sorted.bam") is False:
                return False
        return True

    return False

##################################################################
def mergebam(fq):
    r = get_client()
    cmd = ""

    mfiles = r.smembers('mergebam_'+fq)

    if len(mfiles) > 0:
        #create header
        hf = open(DATA_DIR+'/'+fq+'.header','w')
        for f in mfiles:
            hf.write("@RG\tID:"+f.split('_')[0]+"\tSM:"+f.split('_')[0]+"\tPL:illumina\n")
        #hf.write("@RG\tID:"+f[0].split('_')[0]+"\tSM:"+f[0].split('_')[0]+"\tPL:illumina\n")
        hf.close()
        #merge bams
        cmd = "samtools merge -f -h "+DATA_DIR+"/"+fq+".header "+DATA_DIR+"/"+fq+".sorted.bam "
        for f in mfiles:
            cmd = cmd + DATA_DIR+"/"+f+".sorted.bam "
    #sys.stderr.write(cmd)
    return cmd

##################################################################
def splitbam(fq):    
    pass
    #cmd = "samtools view -h "+DATA_DIR+"/"+fq+".sorted.bam | awk '$3=="chr1" || $3=="chr21" || /^@/' | samtools view -Sb -> "+fq+".1-21.bam"
