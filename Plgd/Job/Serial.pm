package Plgd::Job::Serial;

use strict;
use warnings;
our @ISA = qw(Plgd::Job);  

sub new ($) {   
    my ($cls, $pl, %params) = @_;

    my $self = $cls->SUPER::new($pl, %params); 

    $self->{jobs} = $params{jobs};
    bless $self, $cls;
    return $self;

}

sub run_core($) {
    my ($self) = @_;
        
    Plgd::Logger::info("Job::Serial::run_core $self->{name}");

    foreach my $j (@{$self->{jobs}}) {
        print("jjj $j->{name} jjj\n");
        $j->run();
    }

}

1;