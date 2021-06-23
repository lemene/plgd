
package Plgd::Job::Function;

use strict;
use warnings;
our @ISA = qw(Plgd::Job);  

sub new ($) {   
    my ($cls, $pl, %params) = @_;

    my $self = $cls->SUPER::new($pl, %params); 

    $self->{funcs} = $params{funcs};
    bless $self, $cls;
    return $self;

}

sub submit_core() {
    my ($self) = @_;
    foreach my $f (@{$self->{funcs}}) {
        $f->();
    }
    Plgd::Utils::echo_file($self->get_done_fname(), "0");
}


1;
