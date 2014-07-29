#!/usr/bin/perl
#
#  take a bam file and split it into smaller chunks
#  Useage: splitbam.pl <bamfile>
#
#use strict;
use warnings;
use File::Basename;
use Data::Dumper;
use Storable;

my @filehandles;
my ($fq,$path) = fileparse($ARGV[0]);

my $data = "/fs-research01/tromp/data";
my $bam = "$data/$fq/$fq.bam";

# check if $bam exists, if not use aligned.sorted.bam
if (! -e $bam) {
  $bam = "$data/$fq/$fq.aligned.sorted.bam";
}

my $hashdata = "$data/$fq/$fq.hash";
my $csv = "$data/$fq/$fq.csv";
my $cmd = "samtools view $bam |";

my %h = ();

my %chrs = qw/
1 1
2 2 
3 3
4 4 
5 5 
6 6 
7 7 
8 8 
9 9 
10 10 
11 11
12 12
13 13 
14 14
15 15
16 16
17 17
18 18 
19 19
20 20
21 21 
22 22
23 23
24 24
25 25 
26 26
X 23
Y 24
MT 25
M 25
PAR 26
/;

####################################################################
#
# read through bam and build qname hash file if it doesnt exist
# or load it in if it exists
#
#####################################################################
if (! -e $hashdata) {
  print "creating crossover hash file\n";
  open (BAM,$cmd);
  while (<BAM>) {

    chomp;
    my $line = $_;
    my @parts = split(/\t/,$line);

#  $h{$parts[0]}{'chr'} = $parts[2];
#  push(@{$h{$parts[0]}{'chr'}}, $parts[2]);
    $h{$parts[0]}{$parts[2]} ++;

    if ($h{$parts[0]}{$parts[2]} >= 2) {
      delete $h{$parts[0]};
    }

    if ($. % 1000000 == 0) {
      # ~ 2259000000 - total 
      my @keys = keys %h;
      my $size = @keys;
      print "line $. tracking $size keys\n";
    }

    if ($. == 1000000) {
      last;
    }
  }
  close BAM;              
  store \%h, $hashdata;
} else {
  print "loading crossover hash file\n";
  %h = %{retrieve($hashdata)};
}

####################################################################
# 
# build the csv martix file for the split chr alignments
#
####################################################################
print "building crossover matrix file\n";
my %m = ();
my %rchrs = reverse(%chrs);

foreach my $key (keys(%h)) {
  my @keys = sort keys %{ $h{$key} };
  $keys[0] =~ s/^chr//;
  $keys[1] =~ s/^chr//;
  $m{$chrs{$keys[0]}}{$chrs{$keys[1]}}++;
}

print Dumper(\%m);

open(my $csvfh, ">$csv") || die "cant open csv\n";
print $csvfh "0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26\n";
# create zero'd array
my @counts;
for (my $i=1; $i<=26; $i++) {
  $counts[$i][0] = $i;
  for (my $j=1; $j<=26; $j++) {
    $counts[$i][$j] = 0;
  }
}

for (my $i=1; $i<=26; $i++) {
  for (my $j=1; $j<=26; $j++) {
    if ( exists $m{$i}{$j} ) {
      if ( $i == $j ) {
        $counts[$j][$i] = 0;
      } else {
        $counts[$j][$i] = $m{$i}{$j};
        $counts[$i][$j] = $m{$i}{$j};
      }
    }
  }
}

for (my $i=1; $i<=26; $i++) {
  print $csvfh "$i,";
  for (my $j=1; $j<=26; $j++) {
    if ($j > 1) {print $csvfh ",";}
    print $csvfh $counts[$i][$j];
  }
  print $csvfh "\n";
}

close $csvfh;

##################################################################
#
# let now split the bam and place copies of the split chrs
# into each of the files respectivly 
#
# ###################################################################
print "preforming split opperation\n";
# open out files in respective mux dirs
open(my $chrsplit, ">$data/$fq/$fq.chrsplit") || die "cant open $data/$fq/$fq.chrsplit\n";
for(my $i=1; $i<=26; $i++) {
  `mkdir -p /$data/$fq.$i/`;
  open(my $fh, ">$data/$fq.$i/$fq.$i.sam") || die "cant open $data/$fq.$i/$fq.$i.sam\n";
  push(@filehandles, $fh);
}

# put header from bam in each sam file
foreach my $file (@filehandles) {
  print $file `samtools view -H $bam`;
}

# strem bam data and place in correct sam files
open (BAM,$cmd);
while (<BAM>) {

  chomp;
  my $line = $_;

  if ($line =~ /^\S+\t\S+\t(chr|Chr)?([0-9]{1,2}|X|Y|MT)\t/) {
    my $file;
    my $chr=$2;
    my @parts = split(/\t/,$line);
    
    # if key is in hash write row to two files
    if ( exists $h{$parts[0]} ) {
      #exists as split chr 
      #for each key write to file in @filehandles
      for my $key (keys %{$h{$parts[0]}}) {
        $key =~ s/^chr//;
        $file = $filehandles[$chrs{$key}-1];
        if ($file) {
          print "$parts[2] into key $key\n";
          print $file "$_\n";
        } else {
          print $chrsplit "$_\n";
        }
      }
    } else {
      $file = $filehandles[$chrs{$chr}-1];
      if ($file) {
          print $file "$_\n";
      } else {
          # dump into unmapped file	
          print $chrsplit "$_\n";
      }
    }

  }

}

# close files
close BAM;
close $chrsplit;
foreach my $file (@filehandles) {
    close $file;
}
print "done.\n";
