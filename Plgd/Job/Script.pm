package Plgd::Job::Script;

use strict;
use warnings;
our @ISA = qw(Plgd::Job);  

sub new ($) {   
    my ($cls, $pl, %params) = @_;

    my $self = $cls->SUPER::new($pl, %params); 

    $self->{cmds} = $params{cmds};
    bless $self, $cls;
    return $self;

}

sub submit() {
    my ($self) = @_;
    printf("submit ----\n");
    my $skipped = $self->preprocess();
    if (not $skipped) {
        my $script = $self->get_script_fname();
        $self->write_script($script, $self->{pl}->scriptEnv(), @{$self->{cmds}});
        $self->{submit} = $self->submit_script($script);
        $self->{submit_state} = "running";
    } else {
        $self->postprocess(1);
    }
}


sub poll() {
    my ($self) = @_;

    if ($self->{submit_state} eq "running") {
        my $r = $self->{pl}->{grid}->poll($self->get_script_fname());
        if ($r == 0) {
            Plgd::Script::waitScript($self->get_script_fname(), 60, 5, 1);
            $self->{submit} = undef;
            $self->{submit_state} = "stop";
            $self->postprocess(0);
        }
    }
}


sub run_scripts {
    my ($self, @scripts) = @_;
        
    my $threads = $self->{pl}->get_config("THREADS") + 0;
    my $memroy = $self->{pl}->get_config("MEMORY") + 0;
    my $options = $self->{pl}->get_config("GRID_OPTIONS");
    $self->{pl}->{grid}->run_scripts($threads, $memroy, $options, \@scripts);
}

sub submit_script {
    my ($self, $script) = @_;
        
    my $threads = $self->{pl}->get_config("THREADS") + 0;
    my $memroy = $self->{pl}->get_config("MEMORY") + 0;
    my $options = $self->{pl}->get_config("GRID_OPTIONS");
    $self->{pl}->{grid}->submit_script($script, $threads, $memroy, $options);
}



sub run_core($) {
    my ($self) = @_;
        
    Plgd::Logger::info("Job::Script::run_core $self->{name}");

    my $script = $self->get_script_fname();
    $self->write_script($script, $self->{pl}->scriptEnv(), @{$self->{cmds}});
    $self->run_scripts($script);
    
}


sub write_script {
    my ($self, $fname, $env, @cmds) = @_;
    
    Plgd::Logger::debug("Write Script, $fname");
    #if (! -e $fname) {
    {
        open(F, "> $fname") or die;
        print F "#!/bin/bash\n\n";
        print F "$env";

        print F "retVal=0\n";

        my $wrapCmds = Plgd::Script::wrapCommands(@cmds);
        print F "$wrapCmds\n";

        print F "echo \$retVal > $fname.done\n";
        close(F);

        chmod(0755 & ~umask(), $fname);
    } 
}

1;