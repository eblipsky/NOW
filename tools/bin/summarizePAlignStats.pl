#! /usr/bin/perl -w
use File::Basename;
my $cnt=0; 
my $datadir='/fs-research01/tromp';
my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst)=localtime();
my $datestring=sprintf("%04d%02d%02d-%02d%02d",$year+1900,$mon+1,$mday,$hour,$min);
my $file=$ARGV[0];
my $basename='';
if ( $file =~ /.*(HsMetrics).stats.*/ ){
	$basename=$1."StatsSummary";
} elsif ( $file =~ /.*sortdedup.stats.*/ ){
	$basename="pAlignStatsSummary";
}
#open my $outfh, ">".sprintf("$datadir/stats/%s-%s.txt", $basename, $datestring );
open my $outfh, ">".sprintf("%s-%s.txt", $basename, $datestring );
foreach my $file (@ARGV){
	open my $infh, "<$file"; 
	$cnt++; 
	my ($name,$path,$suffix)=fileparse($file);
	$name=~s/\.sortdedup.stats//; 
	$name=~s/\.HsMetrics.stats//; 
	my $lib=$name; 
	while(<$infh>){
		next if /^$/;
		if ($. == 7){
			if ($cnt==1){
				my @data=();
				@data=split(/\t/,$_); 
				unshift @data, 'FILENAME';
				print $outfh join("\t",@data);
			}
		} elsif ($.>=8){
			my @data=();
			@data=split(/\t/,$_); 
#			$data[0]=$lib; 
			unshift @data, $lib;
			print  $outfh join("\t", @data);
		} 
	}
	close $infh;
}
close $outfh;
