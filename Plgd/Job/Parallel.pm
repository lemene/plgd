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

    foreach my $job (@{$self->{pjobs}}) {
        $job->submit();
    }

    my $count = scalar @{$self->{pjobs}};

    while ($count > 0) {
        $count = 0;
        foreach my $job (@{$self->{pjobs}}) {
            my $r = $job->poll();
            if ($r == 1) {
                $count += 1;
            }
        }
        sleep(5);
    }

    Plgd::Utils::echo_file($self->get_done_fname(), "0");
}

1;