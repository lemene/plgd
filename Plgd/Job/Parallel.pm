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

    foreach my $j (@{$self->{pjobs}}) {
        
        print("jjj $j->{name} jjj\n");
        $j->run();
    }
}

1;