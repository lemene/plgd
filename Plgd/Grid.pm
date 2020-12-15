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
        waiting => {},
        #finished => {},
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


sub submit_script($$$$$) {
    my ($self, $script, $threads, $memory, $options) = @_;

    my $running = $self->{running};
    my $max_jobs = $self->{max_jobs};
    my $waiting = $self->{waiting};

    $waiting->{$script} = [$threads, $memory, $options];
    $self->poll("");
}

# return 0 没有运行 1 表示正在运行
sub poll($$) {
    my ($self, $script) = @_;

    my $waiting = $self->{waiting};
    my $running = $self->{running};
    #my $finished = $self->{finished};
    my $max_jobs = $self->{max_jobs};

    my @finished = ();
    # 首先检查是否有结束的应用
    foreach my $s (keys %$running) {
        my $jobid = $running->{$s};
        my $state = $self->check_script($s, $jobid);
        if ($state eq "" or $state eq "C") {
            push @finished, $s;
        }
    }

    delete @$running{@finished};

    foreach my $s (keys %$waiting) {
        if ($max_jobs == 0 or (keys %$running) < $max_jobs) {
            Plgd::Logger::info("Run script $s");
            my ($threads, $memory, $options) = @{$waiting->{$s}};
            my $r = $self->submit($s, $threads, $memory, $options);
            Plgd::Logger::error("Failed to submit script $s") if (not $r);
            $running->{$s} = $r;
        }
    }

    delete @$waiting{keys %$running};

    if ($script) {
        foreach my $s (keys %$waiting) {
            if ($script eq $s) {
                return 1;
            }
        }
        foreach my $s (keys %$running) {
            if ($script eq $s) {
                return 1;
            }
        }
        return 0;
    }
    return 0;
}

sub run_scripts {
    my ($self, $threads, $memory, $options, $scripts) = @_;

    foreach my $s (@$scripts) {
        $self->submit_script($s, $threads, $memory, $options);
    }

    my $finished = 0;
    while ($finished < scalar @$scripts) {
        $finished = 0;
        foreach my $s (@$scripts) {
            my $r = $self->poll($s);
            printf("run_scripts poll $r\n");
            if ($r == 0) {
                $finished += 1;
            }
        }
        sleep(5);
    }
    Plgd::Script::checkScripts(@$scripts);
}

# sub run_scripts {
#     my ($self, $threads, $memory, $options, $scripts) = @_;


#     my $running = $self->{running};
#     my $max_jobs = $self->{max_jobs};

#     foreach my $s (@$scripts) {
#         Plgd::Logger::info("Run script $s");
#         my $r = $self->submit($s, $threads, $memory, $options);
#         Plgd::Logger::error("Failed to submit script $s") if (not $r);
        
#         $running->{$s} = $r;
#         #my $rsize = keys (%$running);
# 	    if ($max_jobs > 0 and (keys %$running) >= $max_jobs) {
#             my @finished = $self->wait_running(1);
# 	        foreach my $i (@finished) {
#                 delete $running->{$i};
#             }
#             Plgd::Script::checkScripts(@finished);
#         }
        
#     }
#     my @finished = $self->wait_running(0);
#     foreach my $i (@finished) {
#         delete $running->{$i};
#     }
#     Plgd::Script::checkScripts(@finished);
# }


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
            my $state = $self->check_script($s, $jobid);
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
        $self->stop_script($running->{$i});
        delete $running->{$i};
    }
}

1;