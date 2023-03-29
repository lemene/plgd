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

sub submit_core($) {
    my ($self) = @_;
    Plgd::Logger::info("Job::Parallel::submit_core $self->{name}");

    foreach my $job (@{$self->{pjobs}}) {
        $job->submit();
    }
}

sub poll_core($) {
    my ($self) = @_;

    my $count = 0;
    foreach my $job (@{$self->{pjobs}}) {
        my $r = $job->poll();
        if ($r != 0) {
            $count += 1;
        }
    }

    if ($count == 0) {
        Plgd::Utils::echo_file($self->get_done_fname(), "0");
    }
    return $count;
}
1;