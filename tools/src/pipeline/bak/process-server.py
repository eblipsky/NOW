from process import *

r = None

#################################################################
def display_info():

    os.system('clear')

    print ' '
    print '[' + datetime.now().strftime("%Y%m%d %H:%M:%S") + ']'
    print ' '
    print '=== File Info ==='
    for file in r.smembers('fq'):
        print '[' + file  + ']'
        print '- ' + str(r.get('fq_time_' + file))
    print ' ' 
    print '=== Node Info ==='
    for node in r.smembers('nodes'):
        print '[' + node + ']'
        print '- file:' + str(r.get('current_fq_' + node)) 
        print '- stage:' + str(r.get('stage_' + node))
        print '- stage start:' + str(r.get('stage_start_' + node))

##############################################################
#
#############################################################
def Main():
    global r

    print('Start ...')

    # startup redis
    print('connecting to redis...')
    #server_start()
    r = get_client()
    if (r is not None):

        # find files and fill queue
        print('reviewing work dir...')
        review_queues()

        #allow proc nodes to start  
        r.set('working','True')
    
        if (str(sys.argv[1]) != 'serveronly'):
            # startup proc node processes
            print('starting nodes...')
            node_start_all()

        # display info while nodes are working
        active_nodes = len(r.smembers('nodes'))

        while bool(r.get('working')):
            display_info()
            time.sleep(5)

        # all done clean up
        r.expire('working',1)

        #wait for nodes to shutdown before stoping redis
        while (len(r.smembers('nodes')) > 0):
            time.sleep(1)

        #server_stop()
    else:
        print "unable to connect to the redis server"

    print('Finish.')

################################################################
if __name__ == '__main__':
    Main()
