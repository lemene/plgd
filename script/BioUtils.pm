
use strict;


sub writeConfigFile($$) {
    my ($fname, $config) = @_;

    open(F, "> $fname") or die;
    foreach my $item (@$config) {
        print F "$item->[0]=$item->[1]\n";
    }
}

sub jobLink($$$$$) {
    my ($env, $cfg, $name, $ifile, $ofile) = @_;
    
    my $job = Job->new (
        name => "${name}_link",
        ifiles => [$ifile],
        ofiles => [$ofile],
        mfiles => [],
        funcs => [sub ($$) {
            `ln -s -f $ifile $ofile`;
        }],        
        msg => "linking job, $name",
    );
    return $job;
}


sub jobMinimap2Grid($$$$$$$$$) {
    my ($env, $cfg, $name, $target, $query, $options, $blockSize, $overlap, $workDir) = @_;

    mkdir $workDir;

    my $binPath = %$env{"BinPath"};
    my $threads = $cfg->{"THREADS"};
    my $targetIndex = "$workDir/target.index";
    my $queryBlockInfo = "$workDir/query_block_info";
    my $blockPrefix = "query_block";

    my $jobSplit = Job->new(
        name => "${name}_split",
        ifiles => [$query],
        ofiles => [$queryBlockInfo],
        cmds => ["rm -f $workDir/$blockPrefix.*.fasta",
                 "python3 $binPath/utils/split_seqs.py $query $workDir/$blockPrefix.{}.fasta $blockSize",
                 "ls $workDir/$blockPrefix.*.fasta > $queryBlockInfo"],
        msg => "split reads for minimap2, $name",
    );

    my $jobIndex = Job->new(
        name => "${name}_index",
        ifiles => [$target],
        ofiles => [$targetIndex],
        gfiles => [$targetIndex],
        cmds => ["minimap2 -t $threads $options -d $targetIndex $target"],
        msg => "indexing target for minimapw, $name",
    );

    my $jobMap = Job->new(
        prefunc => sub($) {
            my ($job) = @_;
            my $size = `wc -l $queryBlockInfo`;
            for (my $i=0; $i < $size; $i=$i+1) {
                my $subQuery = "$workDir/$blockPrefix.$i.fasta";
                my $subOverlap = "$workDir/$blockPrefix.$i.paf";
                
                my $jobSub = Job->new(
                    name => "${name}_map_$i",
                    ifiles => [$targetIndex, $subQuery],
                    ofiles => ["$subOverlap"],
                    gfiles => ["$subOverlap"],
                    mfiles => [],
                    cmds => ["minimap2 -t $threads $options $targetIndex $subQuery > $subOverlap "] ,
                    msg => "maping reads to index $i, $name", 
                );
                push @{$job->ifiles}, $subQuery;
                push @{$job->ofiles}, $subOverlap;
                push @{$job->pjobs}, $jobSub;
            }

        },
        name => "${name}_map",
        ifiles => [$targetIndex, $queryBlockInfo],
        ofiles => [],                   # prefunc
        mfiles => [],
        pjobs => [],                    # prefunc
        msg => "mapping reads to index using minimap2, $name",
    );

    my $jobCat = Job->new(
        prefunc => sub($) {
            my ($job) = @_;
            my $size = `wc -l $queryBlockInfo`;

            for (my $i=0; $i < $size; $i=$i+1) {
                my $subOverlap = "$workDir/$blockPrefix.$i.paf";
                push @{$job->ifiles}, $subOverlap;
            }
            push @{$job->cmds}, "cat @{$job->ifiles} > $overlap";
        },
        name => "${name}_cat",
        ifiles => [],      # prefunc
        ofiles => [$overlap],
        gfiles => [$overlap],
        mfiles => [],
        cmds => [],                     # prefunc
        msg => "cat minimap2 results, $name",

    );
    
    return Job->new(
        name => "${name}_minimap2",
        ifiles => [$target, $query],
        ofiles => [$overlap], # prefunc
        mfiles => [],
        jobs => [$jobSplit, $jobIndex, $jobMap, $jobCat],
        msg => "mapping query to target using minimap2, $name");
}


sub jobRaconGrid($$$$$$$$$$) {
    my ($env, $cfg, $name, $contigs, $reads, $rd2ctg, $options, $blockSize, $polished, $workDir) = @_;
    mkdir $workDir;

    my $binPath = %$env{"BinPath"};
    my $contigIndex = "$workDir/contig.index";
    my $threads = $cfg->{"THREADS"};
    my $blockInfo = "$workDir/contig_block_info";
    my $blockPrefix = "contig_block";
    my $polishedPrefix = "polished_block";

    my $jobSplit = Job->new(
        name => "${name}_split_ctg",
        ifiles => [$contigs],
        ofiles => [$blockInfo],
        cmds => ["rm -f $workDir/$blockPrefix.*.fasta",
                 "python3 $binPath/utils/split_seqs.py $contigs $workDir/$blockPrefix.{}.fasta $blockSize",
                 "ls $workDir/$blockPrefix.*.fasta > $blockInfo"],
        msg => "split reads, $name",
    );

    my $jobPolish = Job->new(
        prefunc => sub($) {
            my ($job) = @_;
            my $size = `wc -l $blockInfo`;
            for (my $i=0; $i < $size; $i=$i+1) {
                my $subContigs = "$workDir/$blockPrefix.$i.fasta";
                my $subPolished = "$workDir/$polishedPrefix.$i.fasta";
                
                my $jobSub = Job->new(
                    name => "${name}_plsh_$i",
                    ifiles => [$reads, $rd2ctg, $subContigs],
                    ofiles => [$subPolished],
                    gfiles => [$subPolished],
                    mfiles => [],
                    cmds => ["racon -t $threads $options $reads $rd2ctg $subContigs > $subPolished"] ,
                    msg => "polishing contigs $i, $name", 
                );
                push @{$job->ifiles}, $subContigs;
                push @{$job->ofiles}, $subPolished;
                push @{$job->pjobs}, $jobSub;
            }

        },
        name => "${name}_plsh",
        ifiles => [$reads, $rd2ctg, $blockInfo],
        ofiles => [],                   # prefunc
        mfiles => [],
        pjobs => [],                    # prefunc
        msg => "mapping reads to index, $name",
    );

    my $jobCat = Job->new(
        prefunc => sub($) {
            my ($job) = @_;
            my $size = `wc -l $blockInfo`;

            for (my $i=0; $i < $size; $i=$i+1) {
                my $subPolished = "$workDir/$polishedPrefix.$i.fasta";
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
        msg => "cat polishing results, $name",

    );
    
    return Job->new(
        name => "${name}_racon",
        ifiles => [$reads, $contigs, $rd2ctg],
        ofiles => [$polished], # prefunc
        mfiles => [],
        jobs => [$jobSplit, $jobPolish, $jobCat],
        msg => "polishing contigs, $name");

}

sub jobPolishWithMinimap2Racon($$$$$$$$$) {
    my ($env, $cfg, $name, $contigs, $reads, $polished, $options, $blockSize, $workDir) = @_;

    mkdir $workDir;

    my $rd2ctg = "$workDir/rd2ctg.paf";

    my $jobMapContigs = jobMinimap2Grid($env, $cfg, "${name}_map", $contigs, $reads, 
            $options->[0], $blockSize->[0], $rd2ctg, $workDir);
    my $jobPolishContigs = jobRaconGrid($env, $cfg, $name, $contigs, $reads, $rd2ctg, 
            $options->[1], $blockSize->[1], $polished, $workDir);

    return Job->new(
        name => "${name}_job",
        ifiles => [$reads, $contigs],
        ofiles => [$polished], # prefunc
        mfiles => [],
        jobs => [$jobMapContigs, $jobPolishContigs],
        msg => "polishing contigs, $name");


}


1;
