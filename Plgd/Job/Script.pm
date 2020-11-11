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

sub run_core($) {
    my ($self) = @_;
        
    Plgd::Logger::info("Job::Script::run_core $self->{name}");

    my $script = $self->get_script_fname();
    $self->write_script($script, $self->{pl}->scriptEnv(), @{$self->{cmds}});
    $self->{pl}->run_scripts($script);
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