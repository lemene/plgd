#!/usr/bin/env perl

use FindBin;

use Getopt::Long;

$r = `$FindBin::RealBin/run-prj.pl RaconProject.pm @ARGV`;
print($r);