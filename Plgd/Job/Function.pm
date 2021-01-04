
package Plgd::Job::Function;

use strict;
use warnings;
our @ISA = qw(Plgd::Job);  

sub new ($) {   
    my ($cls, $pl, %params) = @_;

    my $self = $cls->SUPER::new($pl, %params); 

    bless $self, $cls;
    return $self;

}

sub run_core() {
    my ($self) = @_;
    foreach my $f (@{$self->{funcs}}) {
        $f->();
    }
}


1;
