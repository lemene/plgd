package Plgd::Job::Parallel;

use strict;
use warnings;
our @ISA = qw(Plgd::Job);  

sub new ($) {   
    my ($cls, $pl, %params) = @_;

    my $self = $cls->SUPER::new($pl, %params); 
    $self->{pjobs} = $params{pjobs};

    bless $self, $cls;
    return $self;
}

sub run_core() {
    my ($self) = @_;
    Plgd::Logger::info("Job::Parallel::run_core $self->{name}");

    #$self->{pl}->parallelRunJobs(@{$self->{pjobs}});

    my @running = ();
    foreach my $job (@{$self->{pjobs}}) {
        $job->submit();
    }

    foreach my $job (@{$self->{pjobs}}) {
        $job->poll();
    }

    Plgd::Utils::echoFile($self->get_done_fname(), "0");
}

1;