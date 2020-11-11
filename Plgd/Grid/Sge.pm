package Plgd::Grid::Sge;

our @ISA = qw(Plgd::Grid);   # inherits from Grid 

use strict;
use warnings;


use File::Basename;
use Plgd::Utils;


sub new ($) {
    my ($cls) = @_;

    if (defined($ENV{'SGE_ROOT'})) {
        my $self = $cls->SUPER::new(); 
        $self->{name} = "sge";
        $self->{path} = $ENV{'SGE_ROOT'};
        bless $self, $cls;
        return $self;
    } else {
        return undef;
    }
}


sub submitScript($$$$$) {

    my ($cls, $script, $thread, $memory, $options) = @_;

    my $jobName = basename($script);

    my $cmd = "qsub -cwd";
    $cmd = $cmd . " -N $jobName";                           # name
    $cmd = $cmd . " -pe smp $thread" if ($thread > 0);      # thread
    $cmd = $cmd . " -l vf=$memory" if ($memory > 0);        # memory
    $cmd = $cmd . " -o $script.log -j yes";                 # output
    $cmd = $cmd . " $options";                              # other options
    $cmd = $cmd . " $script";                               # script
    Plgd::Logger::info("Sumbit command: $cmd");    
    my $result = `$cmd`;
    my @items = split(" ", $result);
    if (scalar @items >= 3) {
        return $items[2];
    } else {
        Plgd::Logger::info("Failed to sumbit command");
    }
}


sub stopScript($$) {
    my ($cls, $job) = @_;
    my $cmd = "qdel $job";
    Plgd::Logger::info("Stop script: $cmd");
    `$cmd`;
}

sub checkScript($$$) {
    my ($cls, $script, $jobid) = @_;
    my $state = "";
    open(F, "qstat |");
    while (<F>) {
        my @items = split(" ", $_);
        if (scalar @items >= 5 and $items[0] eq $jobid) {
            if (grep {$_ eq $items[4]} ("qw", "hqw", "hRwq")) {
                $state = "Q"; 
            } elsif (grep {$_ eq $items[4]} ("r", "t", "Rr", "Rt")) {
                $state = "R"; 
            } elsif (grep {$_ eq $items[4]} ("s", "ts", "S", "tS", "T", "tT", "Rs", "Rts", "RS", "RtS", "RT", "RtT")) {
                $state = "Q";
            } elsif (grep {$_ eq $items[4]} ("Eqw", "Ehqw", "EhRqw", "dr", "dt", "dRr", "dRt", "ds", "dS", "dT", "dRs", "dRS", "dRT")) {
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
1;