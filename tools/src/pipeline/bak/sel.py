#!/usr/bin/python
from process import *

cmd = 'gatk -R ' + REF_HUMAN_GENOME + ' -T SelectVariants --variant ' + DATA_DIR + '/D1B76ACXX8_TTAGGC.raw.vcf -o output3.vcf -L ' + REF_BED 


p = Popen(cmd, shell=True)
p.wait()

