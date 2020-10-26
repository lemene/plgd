package Plgd::ClusterLocal;

use strict;
use warnings;

use File::Basename;

sub create ($) {   
    my ($cls) = @_;

    my $self = {
        name => "local",
        path => "",
    };
    bless $self, $cls;
    return $self;

}

sub submitScript ($$$$$) {
    my ($self, $script, $thread, $memory, $options) = @_;

    my $jobName = basename($script);

    my $pid = 0;
    my $cmd = "$script";                                               # script

    if(!defined($pid = fork())) {
        # fork returned undef, so unsuccessful
        die "Cannot fork a child: $!";
        Plgd::Logger::info("Failed to sumbit command");
    } elsif ($pid == 0) {
        print "Printed by child process\n";
        Plgd::Logger::info("Sumbit command: $cmd");    
        exec($cmd) || die "can't exec date: $!";
    
    } else {
        # fork returned 0 nor undef
        # so this branch is parent
        print "Printed by parent process\n";
        my $ret = waitpid($pid, 0);
        print "Completed process id: $ret\n";

    }
}

sub stopScript($$) {
    my ($self, $job) = @_;
    #Plgd::Logger::info("Stop script: $cmd");
    kill("KILL", $job);
}

sub checkScript($$$) {

    my ($self, $script, $jobid) = @_;
 
    my $r = kill(0, $jobid);
    print("check script $r\n");
    if ($r) {
        return "R";
    } else {
        return "C";
    }
}

1;