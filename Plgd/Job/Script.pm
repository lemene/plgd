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

sub submit_core($) {
    my ($self) = @_;
    my $script = $self->get_script_fname();
    $self->write_script($script, $self->{pl}->scriptEnv(), @{$self->{cmds}});
    $self->{submit} = $self->submit_script($script);
}

sub poll_core($) {
    my ($self) = @_;

    my $r = $self->{pl}->{grid}->poll($self->get_script_fname());
    if ($r == 0) {
        $self->wait_script(60, 5, 1);
        $self->{submit} = undef;
    }
    return $r;
    
}


sub submit_script {
    my ($self, $script) = @_;
        
    my $threads = $self->{pl}->get_config("THREADS") + 0;
    if (defined($self->{threads}) and $self->{threads} > 0) {
        if ( $self->{threads} < $threads) {
            $threads =  $self->{threads};
        }
    }
    my $memroy = $self->{pl}->get_config("MEMORY") + 0;
    my $options = $self->{pl}->get_config("GRID_OPTIONS");
    $self->{pl}->{grid}->submit_script($script, $threads, $memroy, $options);
}



sub write_script {
    my ($self, $fname, $env, @cmds) = @_;
    
    Plgd::Logger::debug("Write Script, $fname");
    #if (! -e $fname) {
    {
        open(F, "> $fname") or die;
        print F "#!/bin/bash\n\n";
        print F "echo \"Plgd script start: \$(date \"+%Y-%m-%d %H:%M:%S\")\"\n";
        print F "$env";

        print F "retVal=0\n";

        my $wrapCmds = wrap_commands(@cmds);
        print F "$wrapCmds\n";

        print F "echo \$retVal > $fname.done\n";
        print F "echo \"Plgd script end: \$(date \"+%Y-%m-%d %H:%M:%S\")\"\n";
        close(F);

        chmod(0755 & ~umask(), $fname);
    } 
}

# wait the scripts is over
# 1: script files
# 2: waiting time
# 3: interval time
# 4: be silent
sub wait_script($$$$) {
    my ($self, $waitTime, $interval, $silent) = @_;
 
    my $script = $self->get_script_fname();

    my $startTime = time();
 
    while (not $self->is_done()) {
        if ($waitTime > 0 and time() - $startTime > $waitTime) {
            return 0;
        }

        if (not $silent) {
            Plgd::Logger::info("Wait script fininshed $script");
        }
        sleep($interval);
    }
    return 1;
}

sub wrap_commands {
    my $str = "";
    foreach my $c (@_) {
        #$str = $str . 
        #       "if [ \$retVal -eq 0 ]; then\n" .
        #       "  $c\n" .
        #       "  temp_result=\$?\n" .
        #       "  if [ \$retVal -eq 0 ]; then\n".
        #       "    retVal=\$temp_result\n" .
        #       "  fi\n" .
        #       "fi\n";
        $str = $str . 
               "if [ \$retVal -eq 0 ]; then\n" .
               "  /usr/bin/time -v $c\n" .
               "  temp_result=(\${PIPESTATUS[*]})\n" .
               "  for i in \${temp_result[*]} \n" .
               "  do\n" .
               "    if [ \$retVal -eq 0 ]; then\n" .
               "      retVal=\$i\n" .
               "    else\n" .
               "      break\n" .
               "    fi\n" .
               "  done\n".
               "fi\n";
    }
    return $str;
}

1;