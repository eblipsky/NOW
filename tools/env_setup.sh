#!/bin/sh

# set this to the base processing dir
export RESEARCH_HOME=/opt/NOW
export JAVA_OPTS='-Xmx50g -Xms50g -Xmn15g -XX:MaxPermSize=500m -Djava.io.tmpdir='$RESEARCH_HOME'/tmp'
# leave the rest alone =]
export TOOLS=$RESEARCH_HOME/tools/bin
export TMPDIR=$RESEARCH_HOME/tmp
export PERL5LIB=$RESEARCH_HOME/tools/src/vcftools_0.1.11/perl
export PATH=$TOOLS:$PATH
export PYTHONPATH=$RESEARCH_HOME/tools/src/variant_tools-2.0.0/lib

# need to up allowed open files per process for whole exome processing
# whole gnome needs way more
ulimit -n 4096
#ulimit -n 81920 

# working with some large files so tweak the command alias for things
alias time='/usr/bin/time -v'
alias df='df -h'
alias du='du -h'
alias ll='ls -lh --color=auto'

