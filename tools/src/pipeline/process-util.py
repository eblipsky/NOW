from process import *

HELP = 'Useage:\n'
HELP += '\tstartup\t\t\t\tstarts redis server\n'
HELP += '\tshutdown\t\t\tstops all nodes and stops redis server\n'
HELP += '\tinstall\t\t\t\tinstall views in couchdb\n'
HELP += '\tprunelog\t\t\tpurge log attachments over 10MB\n'
HELP += '\timport <dir>\t\t\tchecks the dir for new files to import\n'
HELP += '\tstopnode <all|nodeid>\t\tstop node(s)\n'
HELP += '\tstartnode <all|nodeid list>\tstart node(s)\n\t\t\t\t\t* <nodeid list> is space deliminated\n'

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
    elif sys.argv[1] == 'install':
        server = Server(uri=COUCHDB_HOST)
        db = server.get_or_create_db(COUCHDB_DB)
        loader = FileSystemDocsLoader(BASE_DIR+'/tools/src/pipeline/couchdb/_design')
        loader.sync(db, verbose=True)
    elif sys.argv[1] == 'parsecmd':
        print cmd_parse(sys.argv[2:], 'abc_xyz')
    elif sys.argv[1] == 'prunelog':
        prune_log()
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
            for n in range(2, len(sys.argv)):
                node_stop(int(sys.argv[n]))
    elif sys.argv[1] == 'startnode':
        if (sys.argv[2]=='all'):
            node_start_all()
        else:
            for n in range(2, len(sys.argv)):
                node_start(int(sys.argv[n]))
    elif sys.argv[1] == 'import':
        #import_files(sys.argv[2])
        import_files()
    else:
        print HELP
        exit(1)

################################################################
if __name__ == '__main__':
    Main()
