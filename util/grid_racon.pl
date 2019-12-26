#!/usr/bin/env perl

use FindBin;
use lib $FindBin::RealBin;

use Getopt::Long;
use Plgd::Utils;
use Plgd::Script;
use Plgd::Project;
use strict;



sub loadMyConfig() {
    #my () = @_;
    my %cfg = ();
    #loadConfig($fname, \%cfg);
    $cfg{"THREADS"} = 20;
    $cfg{"GRID_NODE"} = 6;
    return %cfg;
}

sub loadMyEnv($) {
    my ($cfg) = @_;

    my %env = loadEnv($cfg);
    $env{"BinPath"} = $FindBin::RealBin;
    return %env;    
}

sub jobRaconPolish($$$$$$$) {
    my ($env, $cfg, $name, $contigs, $reads, $polished, $workDir) = @_;
    mkdir $workDir;
    print "---- $workDir\n";
    my $SplitBin = "~/niefan/MECAT2/Linux-amd64/bin/fsa_rd_tools";
    my $Minimap2Bin = "~/niefan/tool/minimap2/minimap2";
    my $RaconBin = "~/niefan/tool/racon/build/bin/racon"; 

    my $mapOptions = "-x map-ont";
    my $raconOptions = " -m 8 -x -6 -g -8 -w 500";
    my $thread = %$cfg{"THREADS"};

    my $blockSize = 5000000;
    my $blockInfo = "$workDir/block_info";
    my $blockPrefix = "$workDir/contigs";
    my $rd2ctgPrefix = "$workDir/rd2ctg";
 
    my $rd2ctg = "$workDir/rd2ctg.paf";

    my $jobSplit = Job->new(
        name => "${name}_split",
        ifiles => [$reads],
        ofiles => [$blockInfo],
        mfiles => [$blockInfo],
        cmds => ["rm -rf $workDir/$blockPrefix.*.fasta", 
                 "$SplitBin split --ifname $contigs --ofname $blockPrefix.{}.fasta  --block_size $blockSize",
                 "ls $blockPrefix.*.fasta > $blockInfo"],
        msg => "spliting contigs, $name",
    );

    my $jobPolish = Job->new(
        prefunc => sub($) {
            my ($job) = @_;
            my $size = `wc -l $blockInfo`;
            for (my $i=0; $i < $size; $i=$i+1) {
                
                my $subContigs = "$blockPrefix.$i.fasta";
                my $subPolished = "$blockPrefix.$i.polished.fasta";
                my $subRd2ctg = "$rd2ctg.$i.paf";
                my $jobSub = Job->new(
                    name => "${name}_polish_$i",
                    ifiles => [$subContigs, $reads],
                    ofiles => ["$subPolished"],
                    gfiles => ["$subPolished"],
                    mfiles => [],
                    cmds => ["$Minimap2Bin $mapOptions -t $thread $subContigs $reads > $subRd2ctg",
                             "$RaconBin $raconOptions -t $thread $reads $subRd2ctg $subContigs > $subPolished"] ,
                    msg => "polishing contigs $i, $name", 
                );
                push @{$job->ifiles}, $subContigs;
                push @{$job->ofiles}, $subPolished;
                push @{$job->pjobs}, $jobSub;
            }

        },
        name => "cr_correct",
        ifiles => [$blockInfo, $reads],
        ofiles => [],                   # prefunc
        mfiles => [],
        pjobs => [],                    # prefunc
        msg => "polishing reads, $name",
    );

    my $jobCat = Job->new(
        prefunc => sub($) {
            my ($job) = @_;
            my $size = `wc -l $blockInfo`;

            for (my $i=0; $i < $size; $i=$i+1) {
                my $subPolished = "$blockPrefix.$i.polished.fasta";
                push @{$job->ifiles}, $subPolished;
            }

            push @{$job->cmds}, "cat @{$job->ifiles} > $polished";


        },
        name => "${name}_cat",
        ifiles => [],      # prefunc
        ofiles => [$polished],
        gfiles => [$polished],
        mfiles => [],
        cmds => [],                     # prefunc
        msg => "cat polished contigs, $name",

    );
    
    return Job->new(
        name => "${name}_job",
        ifiles => [$contigs, $reads],
        ofiles => [$polished], # prefunc
        mfiles => [],
        jobs => [$jobSplit, $jobPolish, $jobCat],
        msg => "polishing contigs at multiple nodes");

}

sub runRaconPolish($$$$$$) {
    my ($env, $cfg, $count, $reads, $contigs, $workDir) = @_;

    mkdir $workDir;

    my $target = $contigs;

    for (my $i=0; $i<$count; $i=$i+1) {
        my $polished = "$workDir/iter_$i/polished.fasta";
        my $job = jobRaconPolish($env, $cfg, "racon_$i", $target, $reads, $polished, "$workDir/iter_$i");
        serialRunJobs($env, $cfg, $job); 
        $target = $polished;
    }    
}

my %cfg = ();
my %env = ();
sub main() {
    
    %cfg = loadMyConfig();
    %env = loadMyEnv(\%cfg);

    my $reads = @ARGV[0];
    my $contigs = @ARGV[1];

    runRaconPolish(\%env, \%cfg, 4, $reads, $contigs, ".");
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
