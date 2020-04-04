#!/usr/bin/env perl

use FindBin;
use lib $FindBin::RealBin;
use lib $FindBin::RealBin . "/..";

use Getopt::Long;
use Plgd::Utils;
use Plgd::Script;
use Plgd::Project;

use BioUtils;

use strict;


my @defaultConfig = (
    ["PROJECT", ""],
    ["READS", ""],
    ["CONTIGS", ""],
    ["THREADS", "4"],
    ["CLEANUP", "0"],
    ["GRID_NODE", "0"],
    ["READ_BLOCK_SIZE", "4000000000"],
    ["CONTIG_BLOCK_SIZE", "500000000"],
    ["ITERATION_NUMBER", 1],
    ["MINIMAP2_OPTIONS", "-x map-pb"],
    ["RACON_OPTIOINS", ""]
);

sub defaultConfig() {
    my %cfg = ();
    for my $i (0 .. $#defaultConfig){
        $cfg{$defaultConfig[$i][0]} = $defaultConfig[$i][1];
    }
    return %cfg;
}

sub loadRaconConfig($) {
    my ($fname) = @_;
    my %cfg = defaultConfig();
    loadConfig($fname, \%cfg);

    my @required = ("PROJECT", "CONTIGS", "READS");
    foreach my $r (@required) {
        if (not exists($cfg{$r}) or $cfg{$r} eq "")  {
            plgdError("Not set config $r");
        }
    }
    return %cfg;
}


my %cfg = ();
my %env = ();

sub cmdPolish($) {
    my ($fname) = @_;

    %cfg = loadRaconConfig($fname);
    %env = loadEnv(\%cfg);
    initializeProject(\%cfg);
    
    my $prjDir = %env{"WorkPath"} ."/". %cfg{"PROJECT"};
    my $count = %cfg{"ITERATION_NUMBER"};
    my $finalPolished = "$prjDir/polished.fasta";

    my $contigs = $cfg{"CONTIGS"};
    my $reads = $cfg{"READS"};
    my $polished = "";

    for (my $i=0; $i<$count; $i=$i+1) {
        $polished = "$prjDir/iter_$i/polished.fasta";
        my $job = jobPolishWithMinimap2Racon(\%env, \%cfg, "iter_$i", $contigs, $reads, $polished, 
            [$cfg{"MINIMAP2_OPTIONS"}, $cfg{"RACON_OPTIONS"}],
            [$cfg{"READ_BLOCK_SIZE"}, $cfg{"CONTIG_BLOCK_SIZE"}], "$prjDir/iter_$i");
        serialRunJobs(\%env, \%cfg, $job); 
        $contigs = $polished;
    }    

}


sub cmdConfig($) {
    my ($fname) = @_;

    open(F, "> $fname") or die; 
    foreach my $item (@defaultConfig) {
        print F "$item->[0]=$item->[1]\n";
    }

    close(F);

}


sub usage() {
    print "Usage: racon.pl config|polish cfgname\n".
          "    polish:      polish contigs\n" .
          "    config:      generate default config file\n" 
}

sub main() {
    if (scalar @ARGV >= 2) {
        my $cmd = @ARGV[0];
        my $cfgfname = @ARGV[1];

        if ($cmd eq "polish") {
            cmdPolish($cfgfname);
        } elsif ($cmd eq "config") {
            writeConfigFile($cfgfname, \@defaultConfig);
        } else {
            usage();
        }
    } else {
        usage();
    }
}


$SIG{TERM}=$SIG{INT}=\& catchException;
sub catchException { 
    plgdInfo("Catch an Exception, and do cleanup");
    stopRunningScripts(\%env, \%cfg);
    exit -1; 
} 

#eval {
    main();
#};

if ($@) {
    catchException();
}

END {
    stopRunningScripts(\%env, \%cfg);
}
