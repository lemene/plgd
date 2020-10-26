package Plgd::Pipeline;

use strict; 
use warnings;

use Cwd;

use Class::Struct;


use Plgd::Config;
use Plgd::Utils;
use Plgd::Script;
use Plgd::Cluster;

struct Job => {
    prefunc => '$',
    name => '$',
    ifiles => '@',
    ofiles => '@',
    gfiles => '@',
    mfiles => '@',
    cmds => '@',
    jobs => '@',
    pjobs => '@',
    funcs => '@',
    msg => '$',
};

my $WAITING_FILE_TIME = 60;

our %running = ();

sub new {
    my ($cls) = @_;

    my $self = {

    };

    bless $self, $cls;
    return $self;
}


sub initialize($$) {
    my ($self, $fname) = @_;

    
    $self->{cfg} = Plgd::Config::loadConfig($fname);

    $self->{env} = {};
    $self->{env}->{"WorkPath"} = getcwd();
    $self->{env}->{"BinPath"} = $FindBin::RealBin;
    $self->{env}->{"running"} = ();
    $self->{runner} = Plgd::Cluster->create($self->get_config("CLUSTER"));

    # create project folders
    mkdir $self->{cfg}->{"PROJECT"};
    mkdir $self->{cfg}->{"PROJECT"} . "/scripts";

}


sub get_config($$) {
    my ($self, $name) = @_;

    if (exists($self->{cfg}->{$name})) {
        return $self->{cfg}->{$name};
    } elsif (exists($self->{defcfg}->{$name})) {
        return $self->{defcfg}->{$name};
    } else {
    printf("ccz ccc\n");
        #Plgd::Logger::warn("Not recognizes the config: $name");
        return "";
    }
}

sub serialRunJobs {
    my ($self, @jobs) = @_;

    foreach my $job (@jobs) {
        $self->runJob($job);
    }
}


sub runJob ($$) {
    my ($self, $job) = @_;
    
    my $env = $self->{env};
    my $cfg = $self->{cfg};

    $job->prefunc->($job) if ($job->prefunc);
    
    my $prjDir = %$env{"WorkPath"} ."/". %$cfg{"PROJECT"};
   
    my $script = "$prjDir/scripts/" . $job->name. ".sh";

    requireFiles(@{$job->ifiles});
    if (filesNewer($job->ifiles, $job->ofiles) or not isScriptSucc($script)) {
        deleteFiles(@{$job->gfiles}) if ($job->gfiles); 
        deleteFiles("$script.done"); 

        Plgd::Logger::info("Start " . $job->msg . ".") if ($job->msg);

        if (scalar @{$job->cmds} > 0) {
            writeScript($script, $self->scriptEnv($env, $cfg), @{$job->cmds});
            $self->runScript($script);
        } elsif (scalar @{$job->funcs} > 0) {
            foreach my $f (@{$job->funcs}) {
                $f->($env, $cfg);
            }
            echoFile("$script.done", "0");
        } elsif (scalar @{$job->jobs} > 0) {
            foreach my $j (@{$job->jobs}) {
                $self->runJob($j);
            }
            echoFile("$script.done", "0");
        } elsif (scalar @{$job->pjobs} > 0) {
            $self->parallelRunJobs(@{$job->pjobs});
            echoFile("$script.done", "0");
        } else {
            pldgWarn("It is an empty job");
            # die "never come here"
        }

        waitRequiredFiles($WAITING_FILE_TIME, @{$job->ofiles});
        if (%$cfg{"CLEANUP"} == 1) {
            deleteFiles(@{$job->mfiles});
        }

        Plgd::Logger::info("End " .$job->msg . ".") if ($job->msg);
    } else {
        Plgd::Logger::info("Skip ". $job->msg . " for outputs are newer.") if ($job->msg);
    
    }
}


sub parallelRunJobs {
    my ($self, @jobs) = @_;
    
    my $env = $self->{env};
    my $cfg = $self->{cfg};

    my $prjDir = %$env{"WorkPath"} ."/". %$cfg{"PROJECT"};

    # check which job should be run
    my @running = ();
    my @scripts = ();
    foreach my $job (@jobs) {
        
        if (scalar @{$job->funcs} > 0 || scalar @{$job->jobs} > 0) {
            Plgd::Logger::error("Only cmds can run parallel.");
        }

        my $script = "$prjDir/scripts/" . $job->name . ".sh";
        
        requireFiles(@{$job->ifiles});
        if (filesNewer($job->ifiles, $job->ofiles) or not isScriptSucc($script)) {
            unlink @{$job->ofiles};

            writeScript($script, scriptEnv($env, $cfg), @{$job->cmds});
            push @scripts, $script;
            push @running, $job;
        } else {
            Plgd::Logger::info("Skip ". $job->msg . " for outputs are newer.") if ($job->msg);
        }
    }
    
    
    if (scalar @scripts > 0) {
        foreach my $job (@running) {
            Plgd::Logger::info("Parallelly start " . $job->msg . ".") if ($job->msg);
        }

        $self->runScripts(\@scripts);

        foreach my $job (@running) {

            waitRequiredFiles($WAITING_FILE_TIME, @{$job->ofiles});
            
            if (%$cfg{"CLEANUP"} == 1) {
                deleteFiles(@{$job->mfiles});
            }

            Plgd::Logger::info("End " .$job->msg. ".") if ($job->msg);
        }
    }

}


sub scriptEnv($) {
    my ($self) = @_;

    my $env = $self->{env};
    my $cfg = $self->{cfg};

    my $binPath = %$cfg{"BIN_PATH"};

    return "export PATH=$binPath:\$PATH\n";
}



sub runScript($$) {
    my ($self, $script) = @_;
    $self->myrunScripts($script);
}


sub runScripts($$) {
    my ($self, $scripts) = @_;
    
    $self->myrunScripts(@$scripts);
}

sub myrunScripts {
    my ($self, @scripts) = @_;
    
    my $env = $self->{env};
    my $cfg = $self->{cfg};
        
    $self->runScriptsGrid(\@scripts);
}


sub waitScriptsGrid($$$) {
    my ($self, $running, $part) = @_;

    my $env = $self->{env};
    my $cfg = $self->{cfg};

    my @scripts = keys %$running;
    
    my @finished = ();
    until (@finished ~~ @scripts) {
        @finished = ();
        foreach my $s (@scripts) {
            my $jobid = $running{$s};
            my $state = $self->{runner}->checkScript($s, $jobid);
            if ($state eq "" or $state eq "C") {
                if (waitScript($s, 60, 5, 1)) {
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

sub runScriptsGrid($$) {
    my ($self, $scripts) = @_;

    my $env = $self->{env};
    my $cfg = $self->{cfg};
    
    my $node = %$cfg{"GRID_NODE"};

    foreach my $s (@$scripts) {
        Plgd::Logger::info("Run script $s");
        my $r = $self->{runner}->submitScript($s, %$cfg{"THREADS"}, %$cfg{"MEMORY"}, %$cfg{"GRID_OPTIONS"});
        Plgd::Logger::error("Failed to submit script $s") if (not $r);

        $running{$s} = $r;
        my $rsize = keys %running;
	    if ($node > 0 and (keys %running) >= $node) {
            my @finished = $self->waitScriptsGrid(\%running, 1);
	        foreach my $i (@finished) {
                delete $running{$i};
            }
            checkScripts(@finished);
        }
        
    }
    my @finished = $self->waitScriptsGrid(\%running, 0);
    foreach my $i (@finished) {
        delete $running{$i};
    }
    checkScripts(@finished);
    
    
}

sub stopRunningScripts($) {
    my ($self) = @_;

    foreach my $i (keys %running) {
        $self->{running}->stopScript($running{$i});
        delete $running{$i};
    }
}

1;

