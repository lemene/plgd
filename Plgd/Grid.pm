package Plgd::Grid;

use strict; 
use warnings;

use Plgd::Grid::Pbs;
use Plgd::Grid::Local;
use Plgd::Grid::Sge;
use Plgd::Grid::Lsf;
use Plgd::Grid::Slurm;

sub new {
    my ($cls, $max_jobs) = @_;

    my $self = {
        max_jobs =>  $max_jobs,
        running => {},
    };

    bless $self, $cls;
    return $self;
}

sub create($$) {
    my ($cls, $cluster) = @_;       # cluster is "pbs:4"

    my @items = split(":", $cluster);
    my $type = scalar @items >= 1 ? Plgd::Utils::trim($items[0]) : "auto";
    my $max_jobs = scalar @items >= 2 ? $items[1] + 0 : 0;
    my $grid = undef;

    if ($type eq "auto") {
        $grid = Plgd::Grid::Slurm->new($max_jobs) if (not $grid);
        $grid = Plgd::Grid::Pbs->new($max_jobs) if (not $grid);
        $grid = Plgd::Grid::Lsf->new($max_jobs) if (not $grid);
        $grid = Plgd::Grid::Sge->new($max_jobs) if (not $grid);
        $grid = Plgd::Grid::Local->new($max_jobs) if (not $grid);
    } elsif ($type eq "pbs") {
        $grid = Plgd::Grid::Pbs->new($max_jobs) if (not $grid);
    } elsif ($type eq "lsf") {
        $grid = Plgd::Grid::Lsf->new($max_jobs) if (not $grid);
    } elsif ($type eq "sge") {
        $grid = Plgd::Grid::Sge->new($max_jobs) if (not $grid);
    } elsif ($type eq "slurm") {
        $grid = Plgd::Grid::Slurm->new($max_jobs) if (not $grid);
    } elsif ($type eq "local") {
        $grid = Plgd::Grid::Local->new($max_jobs);
    } else {
        Plgd::Logger::error("Not support cluster:  $type");
    }
    
    if (not $grid) {
        Plgd::Logger::error("Not support cluster:  $cluster");
    }

    return $grid;
}


sub submit() {

}

sub run_scripts {
    my ($self, $threads, $memory, $options, $scripts) = @_;


    my $running = $self->{running};
    my $max_jobs = $self->{max_jobs};

    foreach my $s (@$scripts) {
        Plgd::Logger::info("Run script $s");
        my $r = $self->submitScript($s, $threads, $memory, $options);
        Plgd::Logger::error("Failed to submit script $s") if (not $r);
        
        $running->{$s} = $r;
        #my $rsize = keys (%$running);
	    if ($max_jobs > 0 and (keys %$running) >= $max_jobs) {
            my @finished = $self->wait_running(1);
	        foreach my $i (@finished) {
                delete $running->{$i};
            }
            Plgd::Script::checkScripts(@finished);
        }
        
    }
    my @finished = $self->wait_running(0);
    foreach my $i (@finished) {
        delete $running->{$i};
    }
    Plgd::Script::checkScripts(@finished);
}


sub wait_running($$$) {
    my ($self, $part) = @_;
    
    my $running = $self->{running};

    my @scripts = keys %$running;
    my $rsize = keys %$running;

    my @finished = ();
    until (@finished ~~ @scripts) {
        @finished = ();
        foreach my $s (@scripts) {
            my $jobid = $running->{$s};
            my $state = $self->checkScript($s, $jobid);
            if ($state eq "" or $state eq "C") {
                if (Plgd::Script::waitScript($s, 60, 5, 1)) {
                    push @finished, $s
                } else {
                    Plgd::Logger::error("Failed to get script result, id=$jobid, $s")
                }
            } else {
                sleep(5);
            }
        }
        last if ($part and @finished > 0);        
    }
    return @finished;
}

sub stop_all($) {
    my ($self) = @_;

    my $running = $self->{running};
    foreach my $i (keys %$running) {
        $self->stopScript($running->{$i});
        delete $running->{$i};
    }
}

1;