#!/usr/bin/env perl

use FindBin;
use lib $FindBin::RealBin;

use Cwd;
use File::Path qw(make_path remove_tree);
use File::Basename;
use Carp;
use POSIX;

use Plgd::Utils;
use Plgd::Script;
use Plgd::Project;
use FsaUtils;

use Env qw(PATH);

use strict;

my @defaultConfig = (
    ["PROJECT", ""],
    ["READS", ""],
    ["GENOME_SIZE", ""],
    ["THREADS", "4"],
    ["CLEANUP", "0"],
    ["COMPRESS", "0"],
    ["USE_GRID", "false"],
    ["GRID_NODE", "0"],
    ["MIN_READ_LENGTH", "3000"],
    ["PREP_MIN_LENGTH", "2000"],
    ["PREP_OUTPUT_COVERAGE", ""],
    ["CORR_ITERATE_NUMBER", "1"],
    ["CORR_CORRECT_OPTIONS", ""],
    ["CORR_FILTER_OPTIONS", "--max_overhang=2000 --max_overhang_rate=0.20 --min_identity=0"],
    ["CORR_RD2RD_OPTIONS", "-x ava-pb"],
    ["CORR_OUTPUT_COVERAGE", "30"],
    ["ALIGN_RD2RD_OPTIONS", "-X -g3000 -w10 -k19 -m100 -r150 -c"],
    ["ALIGN_FILTER_OPTIONS", ""],
    ["ASM1_FILTER_OPTIONS", ""],
    ["ASM1_ASSEMBLE_OPTIONS", ""],
    ["PHASE_RD2CTG_OPTIONS", "-x map-pb -c"],
    ["PHASE_USE_READS", "0"],
    ["PHASE_PHASE_OPTIONS", "--min_identity=70"],
    ["ASM2_FILTER_OPTIONS", ""],
    ["ASM2_ASSEMBLE_OPTIONS", ""],
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
            plgdError("Not set config $r");
        }
    }
    return %cfg;
}

sub loadMiniasmEnv($) {
    my ($cfg) = @_;

    my %env = loadEnv($cfg);
    $env{"BinPath"} = $FindBin::RealBin;
    return %env;    
}


sub initializeMiniasmProject($) {
    my ($cfg) = @_;

    initializeProject($cfg);
}

sub getReadStatInfo($$) {
    my ($env, $read) = @_;

    my $binPath = %$env{"BinPath"};
    my %info = ();
    my $cmd = "$binPath/fsa_rd_tools n50 --ifname $read |";
    open(my $f, $cmd) or plgdError("Executing error: $cmd"); 
    while ( <$f> ) { 
        my @items = split(":", $_);
        if (scalar @items >= 2) {
            $info{$items[0]} = $items[1];
        }
    } 

    return %info;
}

sub fileLength($) {
    my ($fname) = @_;
    my @args = stat ($fname);
    return $args[7];
}

# copy reads to project and filter out some reads
sub runPrepare($$$$$$) {
    my ($env, $cfg, $name, $workDir, $ifile, $ofile) = @_;

    mkdir $workDir;
    
    my $isGz = ($ofile =~ /\.gz$/);
    my $ofileTemp = $ofile;
    $ofileTemp =~ s/\.gz//;

    my $binPath = $env->{"BinPath"};
    my $threads = $cfg->{"THREADS"};

    my $minLength = $cfg->{"PREP_MIN_LENGTH"} + 0;
    my $baseSize = ($cfg->{"PREP_OUTPUT_COVERAGE"} + 0) * ($cfg->{"GENOME_SIZE"} + 0);
    my $id2name = dirname($ofile) . "/id2name.gz";

    my $oldConfig = switchRunningConfig($cfg, "PREP");

    my $job = Job->new(
        name => "${name}_job",
        ifiles => [$ifile],
        ofiles => [$ofile],
        gfiles => [$ofile],
        mfiles => [],
        cmds => ["$binPath/fsa_rd_tools longest --ifname $ifile --ofname $ofileTemp --base_size $baseSize --min_length $minLength --id2name $id2name --discard_illegal_read"],
        msg => "preparing reads",
    );
    if ($isGz) {
        push @{$job->cmds}, "$binPath/pigz -f -p $threads $ofileTemp";
    }

    serialRunJobs($env, $cfg, $job);

    resumeConfig($cfg, $oldConfig);
}


sub jobCorrect($$$$$$) {
    my ($env, $cfg, $name, $rawreads, $corrected, $workDir) = @_;
    
    my $isGz = ($corrected =~ /\.gz$/);
    
    my $rd2rd = "$workDir/rd2rd.paf";
    my $scores = "$workDir/scores.txt";
    my $ignored = "$workDir/ignored.txt";

    my $binPath = %$env{"BinPath"};
    my $threads = %$cfg{"THREADS"};
    my $readName = "$workDir/readname";

    my $blockSize = 4000000000;
    my $blockInfo = "$workDir/block_info";

 
    my $jobCand = jobRead2Read($env, $cfg, $name, $rawreads, [$cfg->{"CORR_RD2RD_OPTIONS"}, $cfg->{"CORR_FILTER_OPTIONS"}], $rd2rd, $workDir);

    my $jobSplit = Job->new(
        name => "${name}_split",
        ifiles => [$rawreads],
        ofiles => [$blockInfo],
        mfiles => [],
        cmds => ["rm -rf $readName.*", 
                 "$binPath/fsa_rd_tools split_name --ifname $rawreads --ofname $readName.{}  --block_size $blockSize",
                 "ls $readName.* > $blockInfo"],
        msg => "spliting read names, $name",
    );

    my $jobCorr = Job->new(
        prefunc => sub($) {
            my ($job) = @_;
            my $size = `wc -l $blockInfo`;
            for (my $i=0; $i < $size; $i=$i+1) {

                my $corrSub = $isGz ? "$corrected.$i.gz" : "$corrected.$i";
                
                my $jobSub = Job->new(
                    name => "${name}_correct_$i",
                    ifiles => [$rawreads, $rd2rd, "$readName.$i"],
                    ofiles => [$corrSub],
                    gfiles => [$corrSub],
                    mfiles => [],
                    cmds => ["$binPath/fsa_rd_correct $rd2rd $rawreads $corrected.$i --output_directory=$workDir --thread_size=$threads " . 
                                "--score_fname=$scores.$i --ignored_fname=$ignored.$i --read_name_fname=$readName.$i " . $cfg->{"CORR_CORRECT_OPTIONS"}],
                    msg => "correcting reads $i, $name"
                );
                if ($isGz) {
                    push @{$jobSub->cmds}, "$binPath/pigz -p $threads $corrected.$i";
                }
                push @{$job->ofiles}, $corrSub;
                push @{$job->ofiles}, "$scores.$i";
                push @{$job->ofiles}, "$ignored.$i";
                push @{$job->pjobs}, $jobSub;
            }

        },
        name => "${name}_correct_all",
        ifiles => [$blockInfo, $rawreads, $rd2rd],
        ofiles => [],                   # prefunc
        mfiles => [],
        pjobs => [],                    # prefunc
        msg => "correcting rawreads, $name",
    );

    my $jobCat = Job->new(
        prefunc => sub($) {
            my ($job) = @_;
            my $size = `wc -l $blockInfo`;

            my @correctedSub = ();
            my @scoresSub = ();
            my @ignoredSub = ();
            for (my $i=0; $i < $size; $i=$i+1) {
                $correctedSub[$i] = $isGz ? "$corrected.$i.gz" : "$corrected.$i";
                $scoresSub[$i] = "$scores.$i";
                $ignoredSub[$i] = "$ignored.$i";
            }

            push @{$job->ifiles}, @correctedSub, @scoresSub,  @ignoredSub;

            push @{$job->cmds}, "cat @correctedSub > $corrected && rm @correctedSub";
            push @{$job->cmds}, "cat @scoresSub > $scores && rm @scoresSub";
            push @{$job->cmds}, "cat @ignoredSub > $ignored && rm @ignoredSub";


        },
        name => "${name}_cat",
        ifiles => [],      # prefunc
        ofiles => [$corrected, $scores, $ignored], 
        gfiles => [$corrected, $scores, $ignored], 
        mfiles => [],
        cmds => [],                     # prefunc
        msg => "cat corrected reads, $name",

    );
    
    return Job->new(
        name => "${name}_correct",
        ifiles => [$rawreads],
        ofiles => [$corrected], # prefunc
        mfiles => [],
        jobs => [$jobCand, $jobSplit, $jobCorr, $jobCat],
        msg => "correcting rawreads, $name");
    
}

sub runCorrect($$$$$$) {
    my ($env, $cfg, $name, $workDir, $reads, $corrReads) = @_;

    mkdir $workDir;
    
    my $isGz = ($corrReads =~ /\.gz$/);

    my @jobs = ();

    my $baseSize = ($cfg->{"CORR_OUTPUT_COVERAGE"} + 0) * ($cfg->{"GENOME_SIZE"} + 0);
    my $iterNum = $cfg->{"CORR_ITERATE_NUMBER"} + 0;

    my $corrInput = $reads;
    my $corrOutput = $reads;

    
    my $oldCfg = switchRunningConfig($cfg, "CORR");


    for (my $i=0; $i<$iterNum; $i=$i+1) {
        $corrOutput = $isGz ? "$workDir/$i/corrected_reads.fasta_$i.gz" : "$workDir/$i/corrected_reads_$i.fasta";
        push @jobs, jobCorrect($env, $cfg, "crr" . $i, $corrInput, $corrOutput, "$workDir/$i");
        $corrInput = $corrOutput;
    }
    
    if ($baseSize > 0) {
        push @jobs, jobExtract($env, $cfg, $name, $corrOutput, $corrReads, $baseSize);  
    } else {
        printf("$corrOutput \n");
        push @jobs, jobSkip($env, $cfg, "crr", $corrOutput, $corrReads);
    }

    serialRunJobs($env, $cfg, Job->new(
        name => "${name}_job",
        ifiles => [$reads],
        ofiles => [$corrReads],
        mfiles => [],
        jobs => [@jobs],
        msg => "correcting reads, $name",
    ));

    resumeConfig($cfg, $oldCfg);
}

sub runAlign($$$$$$) {
    my ($env, $cfg, $name, $workDir, $corrReads, $overlaps) = @_;
 
    mkdir $workDir;

    my $oldCfg = switchRunningConfig($cfg, "ALIGN");

    runRead2ReadParallelly($env, $cfg, $name, $workDir, $corrReads, [$cfg->{"ALIGN_RD2RD_OPTIONS"}, $cfg->{"ALIGN_FILTER_OPTIONS"}], $overlaps);
    resumeConfig($cfg, $oldCfg);
}

sub runAssemble1($$$$$$) {
    my ($env, $cfg, $name, $workDir, $reads, $overlaps) = @_;
        
    my $oldCfg = switchRunningConfig($cfg, "ASM2");
    runAssemble($env, $cfg, $name, $workDir, $reads, $overlaps, [$cfg->{"ASM1_FILTER_OPTIONS"}, $cfg->{"ASM1_ASSEMBLE_OPTIONS"}]);
    resumeConfig($env, $oldCfg);
}

sub jobFilterPhased($$$$$$$) {
    my ($env, $cfg, $name, $workDir, $overlaps, $ignored, $filtered) = @_;
 
    my $prjDir = %$env{"WorkPath"} . "/" .%$cfg{"PROJECT"};

    my $binPath = %$env{"FsaBinPath"}; 
    my $threads = %$cfg{"THREADS"};

    my $job = Job->new(
        name => "${name}_filter",
        ifiles => [$overlaps, $ignored],
        ofiles => [$filtered],
        gfiles => [$filtered],
        mfiles => [],
        cmds => ["$binPath/fsa_ol_tools filter --thread_size=$threads --ifname=$overlaps --ofname=$filtered --ignored=$ignored --maptype=corname2corname"],
        msg => "filtering overlaps, ${name}",
    );

    return $job;
}

sub statReadN50($$$$) {
    my ($env, $cfg, $seq, $msg) = @_;


    my $binPath = %$env{"FsaBinPath"}; 

    plgdInfo("N50 of $msg: $seq");
    my $cmd = "$binPath/fsa_rd_tools n50 --ifname $seq";
    print $cmd;
    system($cmd);
}

my %cfg = ();
my %env = ();

sub cmdAssemble($) {
    my ($fname) = @_;

    %cfg = loadMiniasmConfig($fname);
    %env = loadMiniasmEnv(\%cfg);
    initializeMiniasmProject(\%cfg);

    my $prjDir = %env{"WorkPath"} . "/" . %cfg{"PROJECT"};
    my $isGz = %cfg{"COMPRESS"};

    my $inputReads = %cfg{"READS"};
    my $prpReads = $isGz ? "$prjDir/0-prepare/prepared_reads.fasta.gz" : "$prjDir/0-prepare/prepared_reads.fasta";
    my $corrReads = $isGz ? "$prjDir/1-correct/corrected_reads.fasta.gz" : "$prjDir/1-correct/corrected_reads.fasta";
    my $overlaps = $isGz ? "$prjDir/2-align/overlaps.paf.gz" : "$prjDir/2-align/overlaps.paf";
    my $contig1 = "$prjDir/3-assemble/contigs.fasta";

    runPrepare(\%env, \%cfg, "ppr", "$prjDir/0-prepare", $inputReads, $prpReads);
    runCorrect(\%env, \%cfg, "crr", "$prjDir/1-correct", $prpReads, $corrReads);
    runAlign(\%env, \%cfg, "al", "$prjDir/2-align", $corrReads, $overlaps);
    runAssemble1(\%env, \%cfg, "asm1", "$prjDir/3-assemble", $corrReads , $overlaps);
    
    statReadN50(\%env, \%cfg, $contig1, "contigs");
}



sub cmdTest($) {
    my ($fname) = @_;

    %cfg = loadFsaConfig($fname);
    %env = loadFsaEnv(\%cfg);
    initializeFsaProject(\%cfg);

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
    print "Usage: necat.pl assemble|config cfg_fname\n".
          "    assemble:    generate contigs\n" .

          "    test:        test\n" .
          "    config:      generate default config file\n" 
}

sub main() {
    if (scalar @ARGV >= 2) {
        my $cmd = @ARGV[0];
        my $cfgfname = @ARGV[1];

        if ($cmd eq "assemble") {
            cmdAssemble($cfgfname);
        } elsif ($cmd eq "test") {
            cmdTest($cfgfname);
        } elsif ($cmd eq "config") {
            cmdConfig($cfgfname);
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

