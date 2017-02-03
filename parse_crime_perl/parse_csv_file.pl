#!/usr/bin/perl
use warnings;

while(<>){
    if (/.*?(\,\d+\-\d+\-\d+\s\b.*?)\,/){
        my $line = $_;
        $line =~ s/$1//g;
        print "$line";
    }
}
