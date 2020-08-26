#!/usr/bin/env perl

printf("adsfadfad\n");
use FindBin;
use lib $FindBin::RealBin;
use lib $FindBin::RealBin . "/..";

use File::Basename;
use Getopt::Long;
use strict;

if (scalar @ARGV >= 1) {
    require $ARGV[0];
    my $prjClass = fileparse($ARGV[0], qr"\..*");
    my $prj = $prjClass->new();

    $prj->run(@ARGV[1..$#ARGV]);

} else {
    printf("run-prj.pl project.pm ...\n");
    exit;
}



$SIG{TERM}=$SIG{INT}=\& catchException;
sub catchException { 
    Plgd::Logger::info("Catch an Exception, and do cleanup");
    #stopRunningScripts(\%env, \%cfg); TODO
    exit -1; 
} 

#eval {
#    main();
#};

if ($@) {
    catchException();
}

END {
    #stopRunningScripts(\%env, \%cfg); TODO
}
