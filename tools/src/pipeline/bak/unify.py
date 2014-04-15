#!/usr/bin/python
from process import *

qfile = 'D1B76ACXX8_TTAGGC'

inbam = DATA_DIR + '/' + qfile + '.bam'
rawvcf = DATA_DIR + '/' + qfile + '.raw.vcf'

cmd = 'gatk -R ' + REF_HUMAN_GENOME + ' -T UnifiedGenotyper --fix_misencoded_quality_scores -nct ' + str(UG_NCT) + ' -nt ' + str(UG_NT) + ' -I ' + inbam + ' --dbsnp ' + REF_DIR +'/' + REF_VCF + ' -o ' + rawvcf + ' --intervals ' + REF_BED +' --annotation QualByDepth --annotation HaplotypeScore --annotation MappingQualityRankSumTest --annotation ReadPosRankSumTest --annotation FisherStrand --annotation GCContent --annotation AlleleBalanceBySample --annotation AlleleBalance -dcov 200 --min_base_quality_score 20 --output_mode EMIT_ALL_SITES --pair_hmm_implementation ORIGINAL'

p = Popen(cmd, shell=True)
p.wait()

