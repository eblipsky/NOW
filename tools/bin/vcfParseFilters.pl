#!/usr/bin/perl

use strict;
use warnings;
use File::Basename;

my ($file,$path) = fileparse($ARGV[0]);
my $fullname = $ARGV[0];

my $FilterColIndex = -1;
my $searching = 1;

my $CountPass = 0;
my $CountNoRead = 0;
my $CountOther = 0;
my $CountTotal = 0;

#print "$file\n$path\n$fullname\n";

open (VCF,$fullname);
while (<VCF>) {

  chomp;
  my $line = $_;

  # find chrom line to get position of filter col 
  if ($line =~ /^#CHROM/) {
    my @fields = split("\t",$line);
    ($FilterColIndex) = grep { $fields[$_] eq "FILTER" } 0..$#fields;
    $searching = 0;
    next;
  }  

  if (! $searching ) {
    # found the CHROM line and got the filter index so start counting
    $CountTotal++;
    my @fields = split("\t",$line);
    if ($fields[$FilterColIndex] eq "PASS" ) {
        $CountPass++;
    } else {
        $CountOther++;
    }
    if ($fields[$#fields] eq "./.") {
        $CountNoRead++;
    }
  }

}

my $pctPass = sprintf '%.2f%%', 100 * ($CountPass/$CountTotal);
my $pctOther = sprintf '%.2f%%', 100 * ($CountOther/$CountTotal);
my $pctNoRead = sprintf '%.2f%%', 100 * ($CountNoRead/$CountTotal);

#print "$file\n";
#print "Pass\t$CountPass\t$pctPass\n";
#print "Other\t$CountOther\t$pctOther\n";
#print "NoRead\t$CountNoRead\t$pctNoRead\n";

print "$file,$CountPass,$CountOther,$CountNoRead\n";


