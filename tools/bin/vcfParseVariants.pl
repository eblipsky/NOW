#! /usr/bin/perl -w
#
# Script to extract lines from merged (family) vcf that have 1 or more observations of the
# ALT allele
#
# input *.merged.21.vcf
# outputs *.merged.21.vf.vcf

my %fileH;
foreach my $file (@ARGV) {
	if ( -f $file ){
		my $outfile=$file;
		#$outfile=~s/(merged\.\d+)/$1\.vf/;
		$outfile=~s/vcf$/parse.vcf/;
		print "extracting from $file into $outfile\n";
		open my $inFh, "<$file";
		open my $outFh, ">$outfile";
		@{$fileH{$file}}=($inFh,$outFh);
	} else {
		print "File $file does not exist\n";
		exit 1;
	}
}

foreach my $file ( keys %fileH){
	my ($inFh, $outFh) = @{$fileH{$file}};
#	print "$inFh\t$outFh\n";
	while (<$inFh>){
		if ( /^#/ || /\t[01]\/1/ ){
			print $outFh $_;
		}
	}
}
