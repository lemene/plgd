package Plgd::Pipeline;

use strict; 
use warnings;

use Cwd;

use Class::Struct;


use Plgd::Config;
use Plgd::Utils;
use Plgd::Script;
use Plgd::Grid;

# short module name
use constant Logger => 'Plgd::Logger';

struct Job => {
    prefunc => '$',
    postfunc => '$',
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

my $WAITING_FILE_TIME = 3;


sub new {
    my ($cls, $defcfg) = @_;

    my $self = {
        $cfg = Plgd::Config->new($defcfg),
    };

    bless $self, $cls;
    return $self;
}


sub initialize($$) {
    my ($self, $fname) = @_;

    
    #$self->{cfg} = Plgd::Config::loadConfig($fname);

    $self->{env} = {};
    $self->{env}->{"WorkPath"} = getcwd();
    $self->{env}->{"BinPath"} = $FindBin::RealBin;
    $self->{running} = {};
    $self->{grid} = Plgd::Grid->create($self->get_config("grid"));

    # create project folders
    mkdir $self->get_project_folder();
    mkdir $self->get_script_folder();

}

sub get_env($$) {
    my ($self, $name) = @_;
    
    if (exists($self->{env}->{$name})) {
        return $self->{env}->{$name};
    } else {
        Plgd::Logger::error("Not recognizes the environment variable: $name");
    }
}

sub get_config($$) {
    my ($self, $name) = @_;
    return $self->{cfg}->get($name);

}

sub get_config2($$$) {
    my ($self, $name0, $name1) = @_;
    return $self->{cfg}->get2($name0, $name1);
}

sub get_script_fname($$) {
    my ($self, $name) = @_;
    return $self->get_script_folder() . "/$name.sh";
}

sub get_script_folder($) {
    my ($self) = @_;
    return $self->get_project_folder() . "/scripts";
}

sub get_project_folder($) {
    my ($self) = @_;
    
    return $self->get_env("WorkPath") ."/". $$self->get_config("PROJECT");
}

sub newjob($$) {
    my ($self, %params) = @_;
    return Plgd::Job->create($self, %params);
}

sub run_job($$) {
    my ($self, $job) = @_;
    $job->run();
}

sub serialRunJobs {
    my ($self, @jobs) = @_;

    foreach my $job (@jobs) {
        $self->runJob($job);
    }
}


sub runJob ($$) {
    my ($self, $job) = @_;

    $job->prefunc->($job) if ($job->prefunc);
    
    my $prjDir = $self->get_project_folder();
    my $script = $self->get_script_fname($job->name);

    requireFiles(@{$job->ifiles});
    if (filesNewer($job->ifiles, $job->ofiles) or not isScriptSucc($script)) {
        deleteFiles(@{$job->gfiles}) if ($job->gfiles); 
        deleteFiles("$script.done"); 

        Plgd::Logger::info("Start " . $job->msg . ".") if ($job->msg);

        if (scalar @{$job->cmds} > 0) {
            writeScript($script, $self->scriptEnv(), @{$job->cmds});
            $self->run_scripts($script);
        } elsif (scalar @{$job->funcs} > 0) {
            foreach my $f (@{$job->funcs}) {
               # $f->($env, $cfg);
               # TODO
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
            Plgd::Logger::warn("It is an empty job");
            # die "never come here"
        }

        waitRequiredFiles($WAITING_FILE_TIME, @{$job->ofiles});
        if ($self->get_config("CLEANUP") == 1) {
            deleteFiles(@{$job->mfiles});
        }

        Plgd::Logger::info("End " .$job->msg . ".") if ($job->msg);
    } else {
        Plgd::Logger::info("Skip ". $job->msg . " for outputs are newer.") if ($job->msg);
    
    }
}


sub parallelRunJobs {
    my ($self, @jobs) = @_;
    

    my $prjDir = $self->get_project_folder();

    # check which job should be run
    my @running = ();
    my @scripts = ();
    foreach my $job (@jobs) {
        
        if (scalar @{$job->funcs} > 0 || scalar @{$job->jobs} > 0) {
            Plgd::Logger::error("Only cmds can run parallel.");
        }

        my $script = $self->get_script_fname($job->name);
        
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

        $self->run_scripts(@scripts);

        foreach my $job (@running) {

            waitRequiredFiles($WAITING_FILE_TIME, @{$job->ofiles});
            
            if ($self->get_config("CLEANUP") == 1) {
                deleteFiles(@{$job->mfiles});
            }

            Plgd::Logger::info("End " .$job->msg. ".") if ($job->msg);
        }
    }

}


sub scriptEnv($) {
    my ($self) = @_;

    return "";

}


sub run_scripts {
    my ($self, @scripts) = @_;
        
    my $threads = $self->get_config("THREADS") + 0;
    my $memroy = $self->get_config("MEMORY") + 0;
    my $options = $self->get_config("GRID_OPTIONS");
    $self->{grid}->run_scripts($threads, $memroy, $options, \@scripts);
    #$self->runScriptsGrid(\@scripts);
}

sub stop_running($) {
    my ($self) = @_;

    if (exists($self->{grid})) {
        $self->{grid}->stop_all();
    }

}

sub help($) {

} 

1;

