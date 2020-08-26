#!/usr/bin/env perl

use FindBin;
use lib $FindBin::RealBin;
use lib $FindBin::RealBin . "/..";

use Cwd;
use File::Path qw(make_path remove_tree);
use File::Basename;
use Carp;
use POSIX;

use Plgd::Utils;
use Plgd::Script;
use Plgd::Project;
use BioUtils;

use Env qw(PATH);

use strict;

my @defaultConfig = (
    ["PROJECT", ""],
    ["READS", ""],
    ["GENOME_SIZE", ""],
    ["THREADS", "4"],
    ["CLEANUP", "0"],
    ["COMPRESS", "0"],
    ["GRID_NODE", "0"],
    ["ASM_MINIMAP2_OPTIONS", "-x ava-pb"],
    ["ASM_READ_BLOCK_SIZE",  4000000000],
    ["ASM_MINIASM_OPTIONS", ""],
    ["PLSH_RACON_OPTIONS", ""],
    ["PLSH_MINIMAP2_OPTIONS", "-x map-pb"],
    ["PLSH_READ_BLOCK_SIZE", 4000000000],
    ["PLSH_CONTIG_BLOCK_SIZE", 500000000],
    ["PLSH_ITERATION_NUMBER", 1],
);

sub defaultConfig() {
    my %cfg = ();
    for my $i (0 .. $#defaultConfig){
        $cfg{$defaultConfig[$i][0]} = $defaultConfig[$i][1];
    }
    return %cfg;
}

sub loadMiniasmConfig($) {
    my ($fname) = @_;
    my %cfg = ();
    loadConfig($fname, \%cfg);

    my @required = ("PROJECT", "GENOME_SIZE", "READS");
    foreach my $r (@required) {
        if (not exists($cfg{$r}) or $cfg{$r} eq "")  {
            Plgd::Logger::error("Not set config $r");
        }
    }
    return %cfg;
}

sub runAssemble($$$$$$) {
    my ($env, $cfg, $name, $workDir, $reads, $contigs) = @_;
    mkdir $workDir;
        
    my $binPath = $env->{"BinPath"};
    my $threads = $cfg->{"THREADS"};
    my $overlaps = "$workDir/rd2rd.paf";
    my $graph = "$workDir/contigs.gfa";

    my $jobMinimap2 = jobMinimap2Grid($env, $cfg, "${name}_al", $reads, $reads, 
        $cfg->{"ASM_MINIMAP2_OPTIONS"}, $cfg->{"ASM_READ_BLOCK_SIZE"}, $overlaps, $workDir);

    my $jobMiniasm = Job->new(
        name => "${name}_job",
        ifiles => [$reads, $overlaps],
        ofiles => [$contigs],
        gfiles => [$contigs],
        mfiles => [],
        cmds => ["miniasm -f $reads $overlaps > $graph",
                 "awk '/^S/{print \">\"\$2\"\\n\"\$3}' $graph > $contigs"],
        msg => "assembling, $name",
    );

    my $job = Job->new(
        name => "${name}_job",
        ifiles => [$reads],
        ofiles => [$contigs],
        gfiles => [],
        mfiles => [],
        jobs => [$jobMinimap2, $jobMiniasm],
        msg => "assembling, $name",
    );

    serialRunJobs($env, $cfg, $job);

}

sub runPolish($$$$$$$) {
    my ($env, $cfg, $name, $workDir, $contigs, $reads, $finalPolished) = @_;
    mkdir $workDir;

    my $count = $cfg->{"PLSH_ITERATION_NUMBER"};

    my $polished = "";
    for (my $i=0; $i<$count; $i=$i+1) {
        $polished = "$workDir/iter_$i/polished.fasta";
        my $job = jobPolishWithMinimap2Racon($env, $cfg, "${name}_$i", $contigs, $reads, $polished, 
            [$cfg->{"PLSH_MINIMAP2_OPTIONS"}, $cfg->{"PLSH_RACON_OPTIONS"}],
            [$cfg->{"PLSH_READ_BLOCK_SIZE"}, $cfg->{"PLSH_CONTIG_BLOCK_SIZE"}], "$workDir/iter_$i");
        serialRunJobs($env, $cfg, $job); 
        $contigs = $polished;
    }    
}

my %cfg = ();
my %env = ();


sub cmdAssemble($) {
    my ($fname) = @_;

    %cfg = loadMiniasmConfig($fname);
    %env = loadEnv(\%cfg);
    initializeProject(\%cfg);

    my $prjDir = %env{"WorkPath"} . "/" . %cfg{"PROJECT"};

    my $reads = %cfg{"READS"};
    my $contigs = "$prjDir/1-asm/contigs.fasta";
    my $polished = "$prjDir/2-plsh/polished_contigs.fasta";

    runAssemble(\%env, \%cfg, "asm", "$prjDir/1-asm", $reads, $contigs);
    runPolish(\%env, \%cfg, "plsh", "$prjDir/2-plsh", $contigs, $reads, $polished);
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
    print "Usage: miniasm.pl assemble|config cfg\n".
          "    assemble:    generate contigs\n" .
          "    config:      generate default config file\n" 
}

sub main() {
    if (scalar @ARGV >= 2) {
        my $cmd = @ARGV[0];
        my $cfgfname = @ARGV[1];

        if ($cmd eq "assemble") {
            cmdAssemble($cfgfname);
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
    Plgd::Logger::info("Catch an Exception, and do cleanup");
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

