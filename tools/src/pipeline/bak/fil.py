#!/usr/bin/python
from process import *

cmd = "gatk -R " + REF_HUMAN_GENOME + " -T VariantFiltration -o output2.vcf --variant " + DATA_DIR + "/MS03372_00000054.raw.vcf --filterExpression 'QD<5.0' --filterName QDFilter --filterExpression 'ABHet>0.75' --filterName ABFilter --filterExpression 'QUAL<=50.0' --filterName QUALFilter --maskName Mask --clusterSize 3 --clusterWindowSize 0"

#cmd = "gatk -R " + REF_HUMAN_GENOME + " -T VariantFiltration -o output.vcf --variant " + DATA_DIR + "/MS07704_00000059.raw.vcf --filterExpression 'QUAL<=50.0' --filterName QUALFilter --maskName Mask"

p = Popen(cmd, shell=True)
p.wait()

