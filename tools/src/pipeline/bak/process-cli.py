from process import *
import signal

CLIENT_VER = '0.0.1'

r = None
qfiles = []
STAGE = ''
start = datetime.now()

###########################################################
def generic_stage(pipeline,queue):

    global qfiles
    global STAGE
    global start

    STAGE = pipeline+'_queue_' + queue
    STAGE_NEXT = pipeline+'_queue_'+ r.get(pipeline+'_queue_out_' + queue)
    STAGE_ERR = pipeline+'_queue_'+ r.get(pipeline+'_queue_err_' + queue)
    ACTIVE = r.get(pipeline+'_queue_active_'+queue)
    QCMD = r.get(pipeline+'_queue_cmd_'+queue)

    if (ACTIVE != 'active'):
        if (STAGE_NEXT == 'None'):
            return RET_NO_WORK

    tmp = r.get(pipeline+'_queue_files_'+queue)
    if tmp is None:
        tmp = ''
    if '%' in tmp:
        STAGE_CPU = eval(tmp[1:])
    else:
        STAGE_CPU = tmp 

    start = datetime.now()

    set_node_info([],'Checking...',start)

    # grab some files
    qfile = ''
    while (qfile != None) and (len(qfiles) < int(STAGE_CPU)):
        qfile = r.lpop(STAGE)
        if qfile != None:
            qfiles.append(qfile)

    if (len(qfiles)==0): return RET_NO_WORK
 
    if (ACTIVE != 'active'):
        for qfile in qfiles:
            set_file_info(qfile,STAGE,start,datetime.now(),'skip')
            r.rpush(STAGE_NEXT , qfile)
        return RET_NO_WORK
   
    if (len(QCMD)!=0):
        commands = []
        for qfile in qfiles:
            cmd = cmd_parse(QCMD,qfile)
            commands.append( cmd )

        set_cmd(commands)
   
        set_node_info(qfiles,STAGE,start)
        processes = [Popen(cmd, shell=True) for cmd in commands]
        for p in processes:
            p.wait()

        for i in range(len(processes)):
            set_file_info(qfiles[i],STAGE,start,datetime.now(),processes[i].returncode)
            if (processes[i].returncode == 0):
                r.rpush(STAGE_NEXT , qfiles[i])
            else:
                r.rpush(STAGE_ERR , qfiles[i])
    else:
        for qfile in qfiles:
            r.rpush(STAGE_NEXT , qfile)

    return RET_OK

###########################################################
def create_sai():

    STAGE = 'fq->sai'
    STAGE_NEXT = 'sai->sam'
    start = datetime.now()

    set_node_info([],'Checking...',start)

    #if there is a fq file grab one
    qfile = r.lpop(STAGE)

    if (qfile == None): return RET_NO_WORK
      
    #in
    fqfile1 = DATA_DIR + '/' + qfile + '_1.fq'
    fqfile2 = DATA_DIR + '/' + qfile + '_2.fq'
    #out
    saifile1 = DATA_DIR + '/' + qfile + '_1.sai'
    saifile2 = DATA_DIR + '/' + qfile + '_2.sai'

    commands = []
    commands.append('bwa aln -t ' + str(MAX_CPU) + ' ' + REF_HUMAN_GENOME + ' ' + fqfile1 + ' > ' + saifile1 )
    commands.append('bwa aln -t ' + str(MAX_CPU) + ' ' + REF_HUMAN_GENOME + ' ' + fqfile2 + ' > ' + saifile2 )

    set_cmd(commands)

    set_node_info(qfile,STAGE+'.1')
    p = Popen(commands[0], shell=True)
    p.wait()

    set_file_info(qfile,STAGE+'.1',start,datetime.now(),p.returncode)
    if (p.returncode != 0):
        return RET_ERROR

    start = datetime.now()

    set_node_info(qfile,STAGE+'.2',start)
    p = Popen(commands[1], shell=True)
    p.wait()

    set_file_info(qfile,STAGE+'.2',start,datetime.now(),p.returncode)
    if (p.returncode != 0):
        return RET_ERROR

    # move to next queue
    r.rpush(STAGE_NEXT, qfile)

    return RET_OK

#############################################################
def sai_to_sam():

    STAGE = 'sai->sam'
    STAGE_NEXT = 'sam->bam'

    start = datetime.now()
    set_node_info([],'Checking...',start)

    commands = []
    qfiles = []
    qfile = ''

    #going with 1/2 max to see the mem req will be
    while (qfile != None) and (len(qfiles) < (MAX_CPU/2)):
        qfile = r.lpop(STAGE)
        if qfile != None:
            qfiles.append(qfile)
    
    if (len(qfiles)==0): return RET_NO_WORK    

    for qfile in qfiles:

        fqfiles = []
        saifiles = []

        #in
        fqfiles.append( DATA_DIR + '/' + qfile + '_1.fq' )
        fqfiles.append( DATA_DIR + '/' + qfile + '_2.fq' )
        saifiles.append( DATA_DIR + '/' + qfile + '_1.sai' )
        saifiles.append( DATA_DIR + '/' + qfile + '_2.sai' )
        #out
        samfile = DATA_DIR + '/' + qfile + '.sam'

        pe = qfile.split('_')[0]
        ghs = qfile.split('_')[1]

        commands.append('bwa sampe -P -r \'@RG\tID:' + pe + '\tSM:' + ghs + '\tPL:illumina\' ' + REF_HUMAN_GENOME + ' ' + saifiles[0] + ' ' + saifiles[1] + ' ' + fqfiles[0] + ' ' + fqfiles[1] + ' > ' + samfile)

    set_cmd(commands)

    set_node_info(qfiles,STAGE,start)
    processes = [Popen(cmd, shell=True) for cmd in commands]
    for p in processes: 
        p.wait()

    for i in range(len(processes)):
        set_file_info(qfiles[i],STAGE,start,datetime.now(),processes[i].returncode)
        if (processes[i].returncode == 0):
            r.rpush(STAGE_NEXT , qfiles[i])
    
    return RET_OK

#############################################################
def sam_to_bam():

    STAGE = 'sam->bam'
    STAGE_NEXT = 'sort_index_bam'

    start = datetime.now()
    set_node_info([],'Checking...',start)

    commands = []
    qfiles = []
    qfile = ''
    while (qfile != None) and (len(qfiles) < MAX_CPU):
        qfile = r.lpop(STAGE)
        if qfile != None:
            qfiles.append(qfile)

    if (len(qfiles)==0): return RET_NO_WORK

    for qfile in qfiles:
        #in
        samfile = DATA_DIR + '/' + qfile + '.sam'
        #out
        bamfile = DATA_DIR + '/' + qfile + '.bam'

        commands.append('samtools view -bSho ' + bamfile + ' ' + samfile)

    set_cmd(commands)

    set_node_info(qfiles,STAGE)
    processes = [Popen(cmd, shell=True) for cmd in commands]
    for p in processes: 
        p.wait()

    for i in range(len(processes)):
        set_file_info(qfiles[i],STAGE,start,datetime.now(),processes[i].returncode)
        if (processes[i].returncode == 0):
            r.rpush(STAGE_NEXT , qfiles[i]) 

#############################################################
def sort_index_bam():

    STAGE = 'sort_index_bam'
    STAGE_NEXT = 'base_recal_print_reads'

    start = datetime.now()
    set_node_info([],'Checking...',start)

    global current_fq
    global current_stage

    commands = []
    commands1 = []
    qfiles = []
    qfile = ''
    while (qfile != None) and (len(qfiles) < MAX_CPU):
        qfile = r.lpop(STAGE)
        if qfile != None:
            qfiles.append(qfile)

    if (len(qfiles)==0): return RET_NO_WORK

    for qfile in qfiles:
        #in
        bamfile = DATA_DIR + '/' + qfile + '.bam'
        sortedbamfilein = DATA_DIR + '/' + qfile + '.sorted.bam'
        #out
        sortedbamfileout = DATA_DIR + '/' + qfile + '.sorted'

        commands.append('samtools sort ' + bamfile + ' ' + sortedbamfileout)
        commands1.append('samtools index ' + sortedbamfilein)

    set_cmd(commands)

    set_node_info(qfiles,STAGE+'.sort')
    processes = [Popen(cmd, shell=True) for cmd in commands]
    for p in processes: 
        p.wait()

    for i in range(len(processes)):
        try:
            set_file_info(qfiles[i],STAGE+'.sort',start,datetime.now(),processes[i].returncode)
            if (processes[i].returncode != 0):
                commands.remove(i)
                commands1.remove(i)
                qfiles.remove(i)
                p.remove(i)
        except:
            pass

    start = datetime.now()

    set_cmd(commands1)

    set_node_info(qfiles,STAGE+'.index',start)
    processes = [Popen(cmd, shell=True) for cmd in commands1]
    for p in processes: 
        p.wait()

    for i in range(len(processes)):
        set_file_info(qfiles[i],STAGE+'.index',start,datetime.now(),processes[i].returncode)
        if (processes[i].returncode == 0):
            r.rpush(STAGE_NEXT , qfiles[i])

    return RET_OK

#############################################################
def base_recal_print_reads():

    STAGE = 'base_recal_print_reads'
    STAGE_NEXT = 'index_bam_report'

    start = datetime.now()
    set_node_info([],'Checking...',start)

    qfile = r.lpop(STAGE)

    if (qfile == None): return RET_NO_WORK

    vcf_files = []
    vcf_files.append(REF_DIR + '/' + REF_VCF)

    vcf_string = ''
    for vcf in vcf_files:
        vcf_string += ' -knownSites ' + vcf

    #in
    sortedbamfile = DATA_DIR + '/' + qfile + '.sorted.bam'
    #out
    recalfile = DATA_DIR + '/' + qfile + '.table'
    finalbamfile = DATA_DIR + '/' + qfile + '.bam' 

    commands = []
    commands.append('gatk -T BaseRecalibrator --fix_misencoded_quality_scores -nct ' + str(MAX_CPU) + ' -I ' + sortedbamfile + ' -R ' + REF_HUMAN_GENOME + ' ' + vcf_string + ' -o ' + recalfile)
    commands.append('gatk -T BaseRecalibrator -nct ' + str(MAX_CPU) + ' -I ' + sortedbamfile + ' -R ' + REF_HUMAN_GENOME + ' ' + vcf_string + ' -o ' + recalfile)
    commands.append('gatk -T PrintReads -nct ' + str(MAX_CPU) + ' -R ' + REF_HUMAN_GENOME + ' -I ' + sortedbamfile + ' -BQSR ' + recalfile + ' -o ' + finalbamfile)

    set_cmd(commands)

    set_node_info(qfile,STAGE+'.base_recal')
    p = Popen(commands[0], shell=True)
    p.wait()

    set_file_info(qfile,STAGE+'.base_recal',start,datetime.now(),p.returncode)

    if (p.returncode != 0):
        # baserecal failed use without --fix-misenc.....
        set_node_info(qfile,STAGE+'.base_recal.noqfix')
        p = Popen(commands[1], shell=True)
        p.wait()    
        set_file_info(qfile,STAGE+'.base_recal.noqfix',start,datetime.now(),p.returncode)

    if (p.returncode != 0):
        return RET_ERROR

    start = datetime.now()

    set_node_info(qfile,STAGE+'.print_reads',start)
    p = Popen(commands[2], shell=True)
    p.wait()

    set_file_info(qfile,STAGE+'.print_reads',start,datetime.now(),p.returncode)

    if (p.returncode != 0):
        return RET_ERROR

    r.rpush(STAGE_NEXT, qfile)

    return RET_OK

#############################################################
def index_bam_report():

    STAGE = 'index_bam_report'
    STAGE_NEXT = 'unify'

    start = datetime.now()
    set_node_info([],'Checking...',start)

    commands = []
    commands1 = []
    qfiles = []
    qfile = ''
    while (qfile != None) and (len(qfiles) < MAX_CPU):
        qfile = r.lpop(STAGE)
        if qfile != None:
            qfiles.append(qfile)

    if (len(qfiles)==0): return RET_NO_WORK

    for qfile in qfiles:

        finalbamfile = DATA_DIR + '/' + qfile + '.bam' 

        commands.append('samtools idxstats ' + finalbamfile)
        commands1.append('samstat -f bam ' + finalbamfile)

    set_cmd(commands)
    
    set_node_info(qfiles,STAGE+'.final_bam_index')
    processes = [Popen(cmd, shell=True) for cmd in commands]
    for p in processes: 
        p.wait()

    for i in range(len(processes)):
        try:
            set_file_info(qfiles[i],STAGE+'.final_bam_index',start,datetime.now(),processes[i].returncode)
            if (processes[i].returncode != 0):
                commands.remove(i)
                commands1.remove(i)
                qfiles.remove(i)
                p.remove(i)
        except:
            pass

    start = datetime.now()

    set_cmd(commands1)

    set_node_info(qfiles,STAGE+'.samstat',start)
    processes = [Popen(cmd, shell=True) for cmd in commands1]
    for p in processes:
        p.wait()

    for i in range(len(processes)):
        set_file_info(qfiles[i],STAGE+'.samstat',start,datetime.now(),processes[i].returncode)
        if (processes[i].returncode == 0):
            r.rpush(STAGE_NEXT , qfiles[i])

    return RET_OK

#############################################################
def unify():
    STAGE = 'unify'
    STAGE_NEXT = 'done'

    start = datetime.now()
    set_node_info([],'Checking...',start)

    commands = []
    commands1 = []
    qfiles = []
    qfile = ''

    while (qfile != None) and (len(qfiles) < (MAX_CPU/4)):
        qfile = r.lpop(STAGE)
        if qfile != None:
            qfiles.append(qfile)

    if (len(qfiles)==0): return RET_NO_WORK

    for qfile in qfiles:

        #in
        inbam = DATA_DIR + '/' + qfile + '.bam'
        #out
        rawvcf = DATA_DIR + '/' + qfile + '.raw.vcf'

        commands.append('gatk -R ' + REF_HUMAN_GENOME + ' -T UnifiedGenotyper --fix_misencoded_quality_scores -nct ' + str(UG_NCT) + ' -nt ' + str(UG_NT) + ' -I ' + inbam + ' --dbsnp ' + REF_DIR +'/' + REF_VCF + ' -o ' + rawvcf + ' --intervals ' + REF_BED +' --annotation QualByDepth --annotation HaplotypeScore --annotation MappingQualityRankSumTest --annotation ReadPosRankSumTest --annotation FisherStrand --annotation GCContent --annotation AlleleBalanceBySample --annotation AlleleBalance -dcov 200 --min_base_quality_score 20 --output_mode EMIT_ALL_SITES --pair_hmm_implementation ORIGINAL')
        commands1.append('gatk -R ' + REF_HUMAN_GENOME + ' -T UnifiedGenotyper -nct ' + str(UG_NCT) + ' -nt ' + str(UG_NT) + ' -I ' + inbam + ' --dbsnp ' + REF_DIR +'/' + REF_VCF + ' -o ' + rawvcf + ' --intervals ' + REF_BED +' --annotation QualByDepth --annotation HaplotypeScore --annotation MappingQualityRankSumTest --annotation ReadPosRankSumTest --annotation FisherStrand --annotation GCContent --annotation AlleleBalanceBySample --annotation AlleleBalance -dcov 200 --min_base_quality_score 20 --output_mode EMIT_ALL_SITES --pair_hmm_implementation ORIGINAL')

    set_cmd(commands)

    set_node_info(qfiles,STAGE)
    processes = [Popen(cmd, shell=True) for cmd in commands]
    for p in processes:
        p.wait()

    for i in range(len(processes)):
        try:
            set_file_info(qfiles[i],STAGE,start,datetime.now(),processes[i].returncode)
            if (processes[i].returncode == 0):
                # this file processed ok move it along
                commands.remove(i)
                commands1.remove(i)
                qfiles.remove(i)
                p.remove(i)
                r.rpush(STAGE_NEXT , qfiles[i])
        except:
            pass

    start = datetime.now()
    set_cmd(commands1)

    set_node_info(qfiles,STAGE+'.nofix',start)
    processes = [Popen(cmd, shell=True) for cmd in commands1]
    for p in processes:
        p.wait()

    for i in range(len(processes)):
        set_file_info(qfiles[i],STAGE+'.nofix',start,datetime.now(),processes[i].returncode)
        if (processes[i].returncode == 0):
            r.rpush(STAGE_NEXT , qfiles[i])

    return RET_OK

#############################################################
def priority_steps():

    if (r.llen('sai->sam') > (MAX_CPU/2)):
        sai_to_sam()
    if (r.llen('sam->bam') > (MAX_CPU)):
        sam_to_bam()
    if (r.llen('sort_index_bam') > (MAX_CPU)):
        sort_index_bam()
    if (r.llen('index_bam_report') > (MAX_CPU)):
        index_bam_report()
    if (r.llen('base_recal_print_reads') > (MAX_CPU)):
        base_recal_print_reads()
    if (r.llen('index_bam_report') > (MAX_CPU)):
        index_bam_report()

#############################################################
def process():

    priority_steps()
    if (create_sai() == RET_OK): return
    if (sai_to_sam() == RET_OK): return
    if (sam_to_bam() == RET_OK): return
    if (sort_index_bam() == RET_OK): return
    if (base_recal_print_reads() == RET_OK): return
    if (index_bam_report() == RET_OK): return
    if (unify() == RET_OK): return

##############################################################
def process_pipeline(pipeline):
    # get pipeline queues
    queues = r.smembers(pipeline+'_queue')
    for q in queues:
        cnt = r.get(pipeline+'_queue_files_'+q)
        if (cnt == None):
            cnt = 0
        if cnt >= MAX_CPU:
            if (r.llen(pipeline+"_queue_"+q) > int(r.get(pipeline+'_queue_files_'+q))):
                generic_stage(pipeline,q)

    for q in queues:
        if (generic_stage(pipeline,q) == RET_OK): return

##############################################################
#
#############################################################
def Main():
    global r

    print('Start ...')
    working = True

    print('connecting to redis...')
    r = get_client()

    print('anounce me....')
    r.sadd('nodes', HOSTNAME)
    r.set('node_ver_'+HOSTNAME, CLIENT_VER)

    print('start working....')
    while bool(r.get('working')):
        #process()
        process_pipeline('cidr_uw')
        time.sleep(1)
    
    print('stoping ...')
    cleanup()
    
    print('Finish.')

#############################################################
def cleanup():
    r.srem('nodes', HOSTNAME)
    r.expire('current_fq_' + HOSTNAME,1)
    r.expire('stage_' + HOSTNAME,1)
    r.expire('node_ver_' + HOSTNAME,1)

#############################################################
def signal_handler(signal, frame):
    for qfile in qfiles:
        set_file_info(qfile,STAGE,start,datetime.now(),'revert')
        r.rpush(STAGE, qfile)
    cleanup()
    sys.exit(0)

#############################################################
if __name__ == '__main__':
    signal.signal(signal.SIGINT, signal_handler)
    Main()
    cleanup()
