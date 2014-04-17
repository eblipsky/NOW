from process import *
import signal

DEBUG = False

CLIENT_VER = '0.0.17'

r = None
STAGE = ''
start = datetime.now()

###########################################################
def generic_stage(pipeline, queue):

    global STAGE
    global start

    # todo: move this to redis and edit on web portal and maybe make per pipeline / batch options
    email_to = 'eblipsky@geisinger.edu'
    email_body = ''
    
    # clear the files from my internal store
    r.ltrim(HOSTNAME+'_files', 1, 0)
    set_cmd()

    start = datetime.now()
    set_node_info('...', start)

    # if we don't have a pipeline or queue do nothing
    if pipeline is None:
        return RET_NO_WORK
    if queue is None:
        return RET_NO_WORK

    # if we are a special queue do nothing
    if queue == 'None':
        return RET_NO_WORK
    
    if queue == 'error':
        fqs = r.lrange(pipeline+'_queue_'+queue, 0, -1)
        for fq in fqs:
            set_batch_info(fq, HOSTNAME, queue, pipeline, 'pipeline error')
        return RET_NO_WORK

    if queue == 'done':
        # check done queue for batch files
        fqs = r.lrange(pipeline+'_queue_'+queue, 0, -1)
        for fq in fqs:
            set_batch_info(fq, HOSTNAME, queue, pipeline, 'pipeline done')
            # is file in batch
            batch = in_batch(fq)
            if batch != 'NoBatch':
                # check if this is the last pipeline
                if r.lindex('batch_'+batch+'_pipelines', -1) != pipeline:
                    set_batch_info(fq, HOSTNAME, queue, pipeline, 'moving')
                    # find next pipeline and push to start queue of pipeline
                    pindex = 0
                    for p in r.lrange('batch_'+batch+'_pipelines', 0, -1):
                        pindex += 1
                        if p == pipeline:
                            break
                    pipeline_next = r.lindex('batch_'+batch+'_pipelines', pindex)
                    r.lrem(pipeline+'_queue_'+queue, fq)
                    r.rpush(pipeline_next+'_queue_start', fq)
                else:
                    set_batch_info(fq, HOSTNAME, queue, pipeline, 'batch finished')
                    r.lrem(pipeline+'_queue_'+queue, fq)
                    email_body += '\nfile:'+fq

            # if we have finished batch files send email
            if email_body != '':
                email_subject = 'Batch Complete: '+batch
                email(email_body, email_subject, email_to)

        return RET_NO_WORK

    start = datetime.now()
    set_node_info('Checking '+pipeline+'...', start)

    # if the out or err queues are not set do nothing
    if r.get(pipeline+'_queue_out_' + queue) is None:
        return RET_NO_WORK
    if r.get(pipeline+'_queue_err_' + queue) is None:
        return RET_NO_WORK

    # get the important vars
    STAGE = pipeline+'_queue_' + queue
    STAGE_NEXT = pipeline+'_queue_' + r.get(pipeline+'_queue_out_' + queue)
    STAGE_ERR = pipeline+'_queue_' + r.get(pipeline+'_queue_err_' + queue)
    ACTIVE = r.get(pipeline+'_queue_active_'+queue)
    VALID = r.get(pipeline+'_queue_valid_'+queue)
    email_subject = 'Failed Files: '+HOSTNAME+' AT '+STAGE

    output_type = r.get(pipeline+'_queue_output_'+queue)
    if output_type == '' or output_type is None:
        output_type = 'single'

    # check if we need to pull the template command
    if r.get(pipeline+'_queue_cmdtype_'+queue) == 'template':
        QCMD = r.hget('tmplcmd', r.get(pipeline+'_queue_template_'+queue))
    else:
        QCMD = r.get(pipeline+'_queue_cmd_'+queue)

    # if inactive do nothing for start and none queue
    if ACTIVE != 'active':
        if STAGE == pipeline+'_queue_start':
            return RET_NO_WORK
        if STAGE_NEXT == pipeline+'_queue_None':
            return RET_NO_WORK

    # get the number of files the queue will try to grab
    tmp = r.get(pipeline+'_queue_files_'+queue)
    if tmp is None:
        STAGE_CPU = 1
    else:
        STAGE_CPU = eval(tmp)

    # update node status
    start = datetime.now()
    set_node_info('Checking '+pipeline+'...', start)

    # grab some files
    qfile = ''
    while qfile is not None and r.llen(HOSTNAME+'_files') < int(STAGE_CPU):
        qfile = r.lpop(STAGE)
        if qfile is not None:
            r.rpush(HOSTNAME+'_files', qfile)
            set_batch_info(qfile, HOSTNAME, queue, pipeline, 'processing')

    # if there were no files to get do nothing
    if r.llen(HOSTNAME+'_files') == 0:
        return RET_NO_WORK

    # if we have files but are inactive push them along to next queue
    if ACTIVE != 'active':
        for qfile in r.lrange(HOSTNAME+'_files', 0, -1):
            #set_file_info(qfile, STAGE, '', start, datetime.now(), '!!SKIPPED!!')
            set_file_info_new(qfile, None, STAGE, '', start, datetime.now(), '!!SKIPPED!!')
            set_batch_info(qfile, HOSTNAME, queue, pipeline, 'skipped')
            r.rpush(STAGE_NEXT, qfile)
        r.ltrim(HOSTNAME+'_files', 1, 0)
        return RET_NO_WORK

    # run the valid check function
    for qfile in r.lrange(HOSTNAME+'_files', 0, -1):
        if eval(VALID+"('"+qfile+"')") is False:
            r.lpush(STAGE, qfile)
            r.lrem(HOSTNAME+'_files', qfile)

    # make sure we have a command to run
    if len(QCMD) != 0:

        # parse and add all the commands
        commands = []
        qfiles = r.lrange(HOSTNAME+'_files', 0, -1)
        for qfile in qfiles:
            cmd = cmd_parse(pipeline, QCMD, qfile)
            commands.append(cmd)

        set_cmd(commands)
        set_node_info(STAGE, start)

        # execute all commands
        processes = []
        logfiles = []
        logfilenames = []
        for idx, cmd in enumerate(commands):

            logdir = BASE_DIR+'/log/'+HOSTNAME+'/'+STAGE
            logfilename = logdir+'/'+qfiles[idx]+'_'+start.strftime(DATE_FMT_FILE)+'.log'

            if not os.path.exists(logdir):
                os.makedirs(logdir)

            logfile = open(logfilename, 'w')
            logfiles.append(logfile)
            logfilenames.append(logfilename)

            # todo: possibly print data for header of log file, or push log to couch after exit

            process = Popen(cmd, shell=True, stdout=logfile, stderr=logfile)
            processes.append(process)

        for p in processes:
            p.wait()

        for logfile in logfiles:
            logfile.close()

        set_cmd()

        # when they are all done move to next or err queue
        for i in range(len(processes)):
            f = r.lindex(HOSTNAME+'_files', i)
            #set_file_info(f, STAGE, commands[i], start, datetime.now(), processes[i].returncode)
            set_file_info_new(f, logfilenames[i], STAGE, commands[i], start, datetime.now(), processes[i].returncode)
            set_batch_info(qfile, HOSTNAME, queue, pipeline, 'queue done')
            if processes[i].returncode == 0:
                if output_type == 'single':
                    r.rpush(STAGE_NEXT, f)
                elif output_type == 'demux':
                    import_demux(STAGE_NEXT, f)
                elif output_type == 'mux':
                    import_mux(STAGE_NEXT, f)
            else:
                r.rpush(STAGE_ERR, f)
                email_body += "\nfile:"+f

        # if we have errored files send email
        if email_body != '':
            email(email_body, email_subject, email_to)
    else:
        # no command to execute, move them along
        if r.llen(HOSTNAME+'_files') != 0:
            for qfile in r.lrange(HOSTNAME+'_files', 0, -1):
                set_batch_info(qfile, HOSTNAME, queue, pipeline, 'queue done')
                if output_type == 'single':
                    r.rpush(STAGE_NEXT, qfile)
                elif output_type == 'demux':
                    # this shouldnt happen!!
                    r.rpush(STAGE_ERR, qfile)
                elif output_type == 'mux':
                    import_mux(STAGE_NEXT, qfile)

    # clear the files from my internal store
    r.ltrim(HOSTNAME+'_files', 1, 0)

    return RET_OK

##############################################################
def process_pipeline():

    pipeline = r.hget(HOSTNAME, 'pipeline')

    if pipeline is None: 
        start = datetime.now()
        set_node_info('...', start)
        return

    if pipeline == "": 
        start = datetime.now()
        set_node_info('...', start)
        return

    if pipeline == "Auto":
        # this is where we need to lookup and loop through pipelies and their queues
        pipeline = r.srandmember('pipeline')
    
    if pipeline == "CommandTemplates":
        return

    # get pipeline queues
    # todo: this is the code that decides which queue to process, needs work
    queues = r.smembers(pipeline+'_queue')
    for q in queues:
        cnt = r.get(pipeline+'_queue_files_'+q)
        if cnt is None:
            cnt = 0
        if cnt >= MAX_CPU:
            if r.llen(pipeline+"_queue_"+q) >= int(eval(r.get(pipeline+'_queue_files_'+q))):
                generic_stage(pipeline, q)

    for q in queues:
        generic_stage(pipeline, q)

#############################################################
def anounce():
    r.sadd('nodes', HOSTNAME)
    r.hset(HOSTNAME, 'ver', CLIENT_VER)

#############################################################
def signal_handler(signal, frame):
    for qfile in r.lrange(HOSTNAME+'_files', 0, -1):
        #set_file_info(qfile, STAGE, start, datetime.now(), '!!REVERT!!')
        set_file_info_new(qfile, None, STAGE, start, datetime.now(), '!!REVERT!!')
        r.rpush(STAGE, qfile)
    cleanup()
    sys.exit(0)

#############################################################
def Main():
    global r

    r = get_client()
    anounce()

    while bool(r.get('working')):
        process_pipeline()
        anounce()
        time.sleep(5)
    
    cleanup()

#############################################################
if __name__ == '__main__':
    signal.signal(signal.SIGINT, signal_handler)
    try:
        Main()
    except Exception as ex:
        sys.stderr.write('====== STAGE='+str(STAGE)+'=======\n')
        sys.stderr.write(traceback.format_exc())
        sys.stderr.write('=================================\n')
        for qfile in r.lrange(HOSTNAME+'_files', 0, -1):
            #set_file_info(qfile, STAGE, '', start, datetime.now(), '!!REVERT!!')
            set_file_info(qfile, None, STAGE, '', start, datetime.now(), '!!REVERT!!')
            r.rpush(STAGE, qfile)
        r.hset(HOSTNAME, 'stage', 'ERROR')
