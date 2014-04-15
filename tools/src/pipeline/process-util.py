from process import *

HELP = 'Useage:\n'
HELP += '\tstartup\t\t\tstarts redis server\n'
HELP += '\tshutdown\t\tstops all nodes and stops redis server\n'
HELP += '\timport <dir>\t\tchecks the dir for new files to import\n'
HELP += '\tstopnode <all|nodeid>\tstop node(s)\n'
HELP += '\tstartnode <all|nodeid>\tstart node(s)\n'

##############################################################
#
#############################################################
def Main():

    r = None

    if len(sys.argv) == 1:
        print HELP
        exit(1)
    
    if sys.argv[1] == 'startup':
        #server_start()
        os.system('start-redis')
    elif sys.argv[1] == 'parsecmd':
        print cmd_parse(sys.argv[2:], 'abc_xyz')
    elif sys.argv[1] == 'enqueue':
        review_queues()
    elif sys.argv[1] == 'shutdown':
        node_stop_all()
        #server_stop()
        os.system('stop-redis')
    elif sys.argv[1] == 'stopnode':
        if sys.argv[2]=='all':
            node_stop_all()
        else:
            node_stop(int(sys.argv[2]))
    elif sys.argv[1] == 'startnode':
        if (sys.argv[2]=='all'):
            node_start_all()
        else:
            node_start(int(sys.argv[2]))
    elif sys.argv[1] == 'import':
        import_files(sys.argv[2])
    else:
        print HELP
        exit(1)

################################################################
if __name__ == '__main__':
    Main()
