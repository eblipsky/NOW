NOW
===

NOW or Node-Oriented Workflow is a dynamic command template workflow engine for high performance distributed computing (HPC) systems. 

NOW has been developed for a RedHat system.

## System Prerequisites
The following can be installed with yum:

* Apache
* PHP
* Python
* [Redis](http://redis.io)
* [CouchDB](http://couchdb.apache.org/)
* [CouchDBKit](http://couchdbkit.org/)

## Installation

* If you prefer to setup a test VM here are the [instructions](https://github.com/eblipsky/NOW/wiki/NOW_VM)

#### Get NOW
```Shell
cd /opt
git clone https://github.com/eblipsky/NOW.git
```

#### Get JDK
You will need to download the Oracle JDK and unpack it into the /tools/src folder. I currently link to version 1.7.0_25 so if you get a different version you will need to fix the symbolic link in /tools/bin.

#### Get Redis
Goto http://redis.io/ and download redis and unpack into tools/src/. Make and link the binaries in /tools/bin. 

#### Get BWA
1. download from http://bio-bwa.sourceforge.net/ into tools/src
2. untar and make
3. fix symbolic link in tools/bin if your version is different than mine

#### Other Tools
Any other tool can be placed in the tools/src folder and compiled like bwa. Linking the binaries in the /tools/bin folder will make that tool available to NOW.

## Configuration
#### Shell Environment
1. edit NOW/tools/env_setup.sh
2. add the following to the end of your .bashrc so your environment is always setup.
```Shell
 . /opt/NOW/tools/env_setup.sh
```

#### Python Environment
1. edit the tools/src/pipeline/process/settings.py and configure the variables at the top for your environment.

#### Web Environment
Go to https://github.com/eblipsky/NOW-web to find out how to setup the web managment portal.

## Useage

The pipeline command is used to start the system and import data files to be processed.
```
Useage:
	startup			starts redis server
	shutdown		stops all nodes and stops redis server
	import <dir>		checks the dir for new files to import
	stopnode <all|nodeid>	stop node(s)
	startnode <all|nodeid>	start node(s)
```
