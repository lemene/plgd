package Plgd::Pipeline;

use strict; 
use warnings;

use Cwd;



use Plgd::Config;
use Plgd::Utils;
use Plgd::Script;
use Plgd::Grid;

# short module name
use constant Logger => 'Plgd::Logger';

my $WAITING_FILE_TIME = 3;


sub new {
    my ($cls, $defcfg) = @_;

    my $self = {
        cfg => Plgd::Config->new($defcfg),
    };

    bless $self, $cls;
    return $self;
}


sub initialize($$) {
    my ($self, $fname) = @_;

    $self->{cfg}->load($fname);

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

sub get_script_folder($) {
    my ($self) = @_;
    return $self->get_project_folder() . "/scripts";
}

sub get_project_folder($) {
    my ($self) = @_;
    
    return $self->get_env("WorkPath") ."/". $self->get_config("PROJECT");
}

sub newjob($$) {
    my ($self, %params) = @_;
    return Plgd::Job->create($self, %params);
}

sub run_jobs {
    my ($self, @jobs) = @_;

    foreach my $job (@jobs) {
        $job->run();
    }
}

sub parallelRunJobs {
    my ($self, @jobs) = @_;
    

    my $prjDir = $self->get_project_folder();

    # check which job should be run
    my @running = ();
    my @scripts = ();
    foreach my $job (@jobs) {

        my $script = $job->get_script_fname();
        
        require_files(@{$job->{ifiles}});
        if (filesNewer($job->{ifiles}, $job->{ofiles}) or not isScriptSucc($script)) {
            unlink @{$job->{ofiles}};

            writeScript($script, $self->scriptEnv(), @{$job->{cmds}});
            push @scripts, $script;
            push @running, $job;
        } else {
            Plgd::Logger::info("Skip ". $job->msg . " for outputs are newer.") if ($job->msg);
        }
    }
    
    
    if (scalar @scripts > 0) {
        foreach my $job (@running) {
            Plgd::Logger::info("Parallelly start " . $job->{msg} . ".") if ($job->{msg});
        }

        $self->run_scripts(@scripts);

        foreach my $job (@running) {

            waitRequiredFiles($WAITING_FILE_TIME, @{$job->{ofiles}});
            
            if ($self->get_config("CLEANUP") == 1) {
                deleteFiles(@{$job->{mfiles}});
            }

            Plgd::Logger::info("End " .$job->{msg}. ".") if ($job->{msg});
        }
    }

}


sub scriptEnv($) {
    my ($self) = @_;

    return "";

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

