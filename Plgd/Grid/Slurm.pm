package Plgd::Grid::Slurm;

use Plgd::Grid;
our @ISA = qw(Plgd::Grid);   # inherits from Grid 

use strict;
use warnings;

use File::Basename;
use Plgd::Utils;

sub new ($$) {
    my ($cls, $max_jobs) = @_;
    
    my $self = $cls->SUPER::new($max_jobs); 

    my $path = `which sinfo 2> /dev/null`;
    $path = trim($path);

    if (not $path eq "") {
        my $self = $cls->SUPER::new(); 
        $self->{name} = "slurm";
        $self->{path} = $path;

        bless $self, $cls;
        return $self;
    } else {
        return undef;
    }
}


sub submit ($$$$$) {
    my ($self, $script, $thread, $memory, $options) = @_;

    my $jobName = basename($script);

    my $cmd = "sbatch -D `pwd`";
    $cmd = $cmd . " -J $jobName";                                           # name
    $cmd = $cmd . " --cpus-per-task=$thread"  if ($thread > 0);             # thread
    $cmd = $cmd . " --mem-per-cpu=$memory" if ($memory > 0);                # memory
    $cmd = $cmd . " -o $script.log";                                        # output
    $cmd = $cmd . " $options";                                              # other options
    $cmd = $cmd . " $script";                                               # script
    Plgd::Logger::info("Sumbit command: $cmd");    
    my $result = `$cmd`;

    my @items = split(" ", $result);
    if (scalar @items >= 4) {
        return $items[3];
    } else {
        Plgd::Logger::info("Failed to sumbit command");
    }
}

sub stop_script($) {
    my ($self, $job) = @_;
    my $cmd = "scancel $job";
    Plgd::Logger::info("Stop script: $cmd");
    `$cmd`;
}

sub check_script($$$) {
    my ($self, $script, $jobid) = @_;
    my $state = "";
    open(F, "squeue |");
    while (<F>) {
        my @items = split(" ", $_);
        if (scalar @items >= 5 and $items[0] eq $jobid) {
            if ($items[4] eq "BF") {
                $state = "C";
            } elsif ($items[4] eq "CA") {
                $state = "C";
            } elsif ($items[4] eq "CD") {
                $state = "C";
            } elsif ($items[4] eq "CF") {
                $state = "Q";
            } elsif ($items[4] eq "CG") {
                $state = "R";
            } elsif ($items[4] eq "DL") {
                $state = "C";
            } elsif ($items[4] eq "F") {
                $state = "C";
            } elsif ($items[4] eq "NF") {
                $state = "C";
            } elsif ($items[4] eq "OOM") {
                $state = "C";
            } elsif ($items[4] eq "PD") {
                $state = "Q";
            } elsif ($items[4] eq "PR") {
                $state = "C";
            } elsif ($items[4] eq "R") {
                $state = "R";
            } elsif ($items[4] eq "RD") {
                $state = "Q";
            } elsif ($items[4] eq "RF") {
                $state = "Q";
            } elsif ($items[4] eq "RH") {
                $state = "Q";
            } elsif ($items[4] eq "RQ") {
                $state = "Q";
            } elsif ($items[4] eq "RS") {
                $state = "Q";
            } elsif ($items[4] eq "RV") {
                $state = "Q";
            } elsif ($items[4] eq "SI") {
                $state = "Q";
            } elsif ($items[4] eq "SE") {
                $state = "Q";
            } elsif ($items[4] eq "SO") {
                $state = "Q";
            } elsif ($items[4] eq "ST") {
                $state = "Q";
            } elsif ($items[4] eq "S") {
                $state = "Q";
            } elsif ($items[4] eq "TO") {
                $state = "C";
            } else {
                $state = "";
            }
            last;
        }
    }
    close(F);
    return $state;
}
