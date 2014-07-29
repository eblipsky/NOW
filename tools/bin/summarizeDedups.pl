#! /usr/bin/perl -w
my $cnt=0; 
my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst)=localtime();
open my $outfh, ">".sprintf("dedupSummary-%04d%02d%02d-%02d%02d.txt", $year+1900,$mon,$mday,$hour,$min);
foreach my $file (@ARGV){
	open my $infh, "<$file"; 
	$cnt++; 
	my $lib=$file; 
	$lib=~s/\.dedup.stats.txt//; 
	while(<$infh>){
		if ($. == 7){
			if ($cnt==1){
				print $outfh $_;
			}
		}elsif($.==8){
			my @data=split(/\t/); 
			$data[0]=$lib; 
			print  $outfh join("\t", @data);
		} 
	}
	close $infh;
}
close $outfh;
