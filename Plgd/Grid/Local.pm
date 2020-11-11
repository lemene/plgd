package Plgd::Grid::Local;

use strict;
use warnings;
use POSIX ":sys_wait_h";
use File::Basename;

our @ISA = qw(Plgd::Grid);  

sub new ($) {   
    my ($cls) = @_;

    my $self = $cls->SUPER::new(); 
    $self->{name} = "local";
    $self->{path} = "";
    $self->{max_jobs} = 1;
    bless $self, $cls;
    return $self;

}

sub submitScript ($$$$$) {
    my ($self, $script, $thread, $memory, $options) = @_;

    printf("script $script\n");
    printf("thread $thread\n");

    printf("memory $memory\n");
    printf("options $options\n");

    my $jobName = basename($script);

    my $pid = 0;
    my $cmd = "$script 2>&1 | tee $script.log";                                               # script

    if(!defined($pid = fork())) {
        # fork returned undef, so unsuccessful
        Plgd::Logger::error("Failed to sumbit command");
    } elsif ($pid == 0) {
        Plgd::Logger::info("Sumbit command: $cmd");    
        system($cmd);
        exit(0);
    
    } else {
        return $pid;
    }
}

sub stopScript($$) {
    my ($self, $job) = @_;
    kill("KILL", $job);
}

sub checkScript($$$) {
    my ($self, $script, $jobid) = @_;
 
    my $r = waitpid($jobid, WNOHANG) ;
    if ($r == 0) {
        return "R";
    } else {
        return "C";
    }
}

1;