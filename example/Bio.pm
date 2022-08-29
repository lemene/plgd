
use strict;


sub jobSkip($$$$$) {
    my ($env, $cfg, $name, $ifile, $ofile) = @_;
    
    my $job = Job->new (
        name => "${name}_skip",
        ifiles => [$ifile],
        ofiles => [$ofile],
        mfiles => [],
        funcs => [sub ($$) {
            `ln -s -f $ifile $ofile`;
        }],        
        msg => "skipping job, $name",
    );
    return $job;
}

sub jobExtract($$$$$$) {
    my ($env, $cfg, $name, $ifname, $ofname, $basesize) = @_;

    my $binPath = %$env{"BinPath"};
    my $threads = %$cfg{"THREADS"};

    my $isGz = ($ofname =~ /\.gz$/);
    my $ofnTemp = $ofname;
    $ofnTemp =~ s/\.gz//;
    
    my @cmds = ();
    if ($basesize > 0) {
        push @cmds, "$binPath/fsa_rd_tools longest $ifname $ofnTemp --base_size=$basesize";
    } else {
        push @cmds, "$binPath/fsa_rd_tools copy --ifname=$ifname --ofname=$ofnTemp";
    }

    if ($isGz) {
        push @cmds, "$binPath/pigz -p $threads $ofnTemp"
    }

    my $job = Job->new(
        name => "${name}_extract",
        ifiles => [$ifname],
        ofiles => [$ofname],
        gfiles => [$ofname],
        mfiles => [],
        cmds => [@cmds],
        msg => "extracting longest reads, $name"
    );

    return $job;
}

sub jobRead2Read($$$$$$$) {
    my ($env, $cfg, $name, $reads, $options, $rd2rd, $workDir) = @_;

    mkdir $workDir;

    my $isGz = ($rd2rd =~ /\.gz$/);

    my $binPath = %$env{"BinPath"};
    my $threads = %$cfg{"THREADS"};

    my $alignOptions = $options->[0];
    my $filterOptions = $options->[1];

    my $cmdSub = $isGz ? " | $binPath/pigz -c -p $threads > $rd2rd" : " > $rd2rd";
    my $job = Job->new(
        name => "${name}_rd2rd",
        ifiles => [$reads], 
        ofiles => [$rd2rd], 
        mfiles => [],
        cmds => ["minimap2 $alignOptions -t $threads $reads $reads | $binPath/fsa_ol_cut - - --itype paf --otype paf $filterOptions $cmdSub"],
    
        msg => "mapping reads",
    );
    return $job;

                   
}

sub runRead2ReadParallelly($$$$$$$) {
    my ($env, $cfg, $name, $workDir, $reads, $options, $rd2rd) = @_;

    mkdir $workDir;

    my $isGz = ($rd2rd =~ /\.gz$/);

    my $binPath = %$env{"BinPath"};
    my $threads = %$cfg{"THREADS"};

    my $min_aligned_length = %$cfg{"MIN_ALIGNED_LENGTH"} + 0;
    my $blocksize = 2000000000;
    my $alignOptions = $options->[0];
    my $filterOptions = $options->[1];

    my $readsIndex = "$workDir/reads.index";
    my $jobIndex = Job->new(
        name => "${name}_index",
        ifiles => [$reads],
        ofiles => [$readsIndex],
        gfiles => [$readsIndex],
        mfiles => [],
        cmds => ["minimap2 -t $threads $alignOptions -d $readsIndex $reads"],
        msg => "indexing reads, $name"
    );

    my $jobSplit = Job->new(

        prefunc => sub ($) {
            my ($job) = @_;

            my $filesize = (stat $reads)[7];
            my $size = int(($filesize + $blocksize - 1) / $blocksize);

            for (my $i=0; $i < $size; $i=$i+1) {
                push @{$job->ofiles}, "$workDir/cc${i}.fasta";
                push @{$job->gfiles}, "$workDir/cc${i}.fasta";
            }
        },
        name => "${name}_split",
        ifiles => [$reads],
        ofiles => [],   # prefunc
        gfiles => [],   # prefunc
        mfiles => [],
        cmds => ["$binPath/fsa_rd_tools split $reads $workDir/cc{}.fasta --block_size=$blocksize"],
        msg => "spliting reads, $name",
    );

    my $jobAlign = Job->new(
        prefunc => sub ($) {
            my ($job) = @_;
            @{$job->ifiles} = @{$jobSplit->ofiles};

            my @subJobs = ();
            for my $i (0..$#{$job->ifiles}) {
                my $f = $jobSplit->ofiles->[$i];
                
                my $ofile = $isGz ? "$f.paf.gz" : "$f.paf";
                my $cmdSub = $isGz ? " | $binPath/pigz -c -p $threads > $ofile" : " > $ofile";
                my $jobSub = Job->new(
                    name => "${name}_align_$i",
                    ifiles => [$f],
                    ofiles => [$ofile],
                    gfiles => [$ofile],
                    mfiles => [],
                    cmds => ["minimap2 $alignOptions -t $threads $readsIndex $f | $binPath/fsa_ol_cut - - --itype paf --otype paf $filterOptions $cmdSub"],
                    msg => "aligning reads $i, $name"
                );

                push @{$job->pjobs}, $jobSub;
                push @{$job->ofiles}, $ofile;
            }

        },
        name => "${name}_align",
        ifiles => [], #prefunc
        ofiles => [], # prefunc
        mfiles => [],
        pjobs => [], # prefunc
        msg => "aligning reads, $name",
    );

    my $jobCat = Job->new(
        prefunc => sub ($) {
            my ($job) = @_;

            @{$job->ifiles} = @{$jobAlign->ofiles};
            push @{$job->cmds}, "cat @{$jobAlign->ofiles} > $rd2rd && rm @{$jobAlign->ofiles}";

        },
        name => "${name}_cat",
        ifiles => [], #prefunc
        ofiles => [$rd2rd], 
        gfiles => [$rd2rd],
        mfiles => [],
        cmds => [],     # prefunc
        msg => "cat overlaps, $name",

    );

    run_jobs($env, $cfg, Job->new(
        name => "${name}_job",
        ifiles => [$reads],
        ofiles => [$rd2rd],
        mfiles => ["$workDir/cc*.fasta", $readsIndex, "$workDir/cc*.fasta.paf", "$workDir/cc*.fasta.paf.gz"],
        jobs => [$jobSplit, $jobIndex, $jobAlign, $jobCat],
        msg => "aligning reads to reads, $name",
    ));
}


sub jobRead2Contig($$$$$$$) {
    my ($env, $cfg, $name, $workDir, $reads, $contigs, $options) = @_;

    mkdir $workDir;

    my $threads = %$cfg{"THREADS"};

    my $read2ctg = "$workDir/rd2ctg.paf";

    my $job = Job->new(
        name => "${name}_rd2ctg",
        ifiles => [$reads, $contigs],
        ofiles => [$read2ctg],
        gfiles => [$read2ctg],
        mfiles => [],
        cmds => ["minimap2  -t $threads $options $contigs $reads > $read2ctg"],
        msg => "mapping reads to contigs, ${name}",
    );

    return $job;
}


sub jobPhase($$$$$$$) {
    my ($env, $cfg, $name, $workDir, $reads, $contigs, $ol_r2c) = @_;
 
    my $phased = "$workDir/phased";
    my $ignored = "$workDir/ignored";
    my $variant = "$workDir/variants";
    my $rdinfo = "$workDir/read_infos";

    my $binPath = %$env{"FsaBinPath"}; 
    my $threads = %$cfg{"THREADS"};
    my $options = $cfg->{"PHASE_PHASE_OPTIONS"};

    my $job = Job->new(
        name => "${name}_phase",
        ifiles => [$reads, $contigs, $ol_r2c],
        ofiles => [$ignored],
        gfiles => [$ignored],
        mfiles => [],
        cmds => ["$binPath/fsa_rd_haplotype $options --thread_size=$threads --ctg_fname=$contigs --rd_fname=$reads --ol_fname=$ol_r2c --output_directory=$workDir" . 
                    " --ignored_fname=$ignored --rdinfo_fname=$rdinfo --var_fname=$variant"],
        msg => "phasing reads with contigs, $name",
    );

    return $job;
}


sub runAssemble($$$$$$$) {
    my ($env, $cfg, $name, $workDir, $reads, $overlaps, $options) = @_;

    mkdir $workDir;

    my $filtered = "$workDir/filter.m4a";
    my $contigs = "$workDir/contigs.fasta";
    my $ignored = $name eq "asm2" ? "$workDir/../4-phase/ignored" : "";
 
    my $binPath = %$env{"FsaBinPath"}; 
    my $thread = %$cfg{"THREADS"};
    my $filterOptions = $options->[0];
    if (%$cfg{"GENOME_SIZE"}) {
        $filterOptions = $filterOptions . " --genome_size=" . %$cfg{"GENOME_SIZE"};
    }
    my $assembleOptions = $options->[1];

    my $jobFilter = Job->new(
        name => "${name}_filter",
        ifiles => [$overlaps],
        ofiles => [$filtered],
        gfiles => [$filtered],
        mfiles => [],
        cmds => ["$binPath/fsa_ol_filter $overlaps $filtered --thread_size=$thread --output_directory=$workDir --read_file=$reads $filterOptions"],
        msg => "filtering overlaps, $name",
    );


    my $jobAssemble = Job->new(
        name => "${name}_asseemble",
        ifiles => [$filtered, $reads],
        ofiles => [$contigs],
        gfiles => [$contigs],
        mfiles => [],
        cmds => ["$binPath/fsa_assemble $filtered --read_file=$reads --thread_size=$thread --output_directory=$workDir $assembleOptions --ignored=$ignored",
                 "gzip -d -f $workDir/graph_edges.gz",
                 ],#"python3 $binPath/tool.py tl_sg2csv $workDir/graph_edges $workDir/asm.csv \"active\""],
        msg => "assembling, $name",
    );

    run_jobs($env, $cfg, Job->new(
        name => "${name}_job",
        ifiles => [$reads, $overlaps],
        ofiles => [$contigs],
        mfiles => [],
        jobs => [$jobFilter, $jobAssemble],
        msg => "assembling job, $name",
    ));
}

1;
