from process import *

DIR = BASE_DIR + '/bak/'

for file in os.listdir(DIR):
    if ( file.count('_') > 2):
        fname, ext = os.path.splitext(file)
        newname = fname.replace('_','',1)
        os.rename(DIR + file, DIR + newname + '.fq')
        #print DIR+file + '->' + DIR + newname + '.fq'

