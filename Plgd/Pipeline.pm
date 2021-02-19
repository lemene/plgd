package Plgd::Pipeline;

use strict; 
use warnings;

use Cwd;



use Plgd::Config;
use Plgd::Utils;
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

sub get_work_folder($$) {
    my ($self, $folder) = @_;
    return $self->get_project_folder() . "/$folder";
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

