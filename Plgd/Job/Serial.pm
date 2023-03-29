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

sub submit_core($) {
    my ($self) = @_;
    
    Plgd::Logger::info("Job::Serial::submit_core $self->{name}");
    
    $self->{index} = 0;
    my $index = $self->{index};
    if ($index < scalar @{$self->{jobs}}) {
        $self->{jobs}->[$index]->submit();
    }
}

sub poll_core($) {
    my ($self) = @_;

    my $index = $self->{index};
    my $count = scalar @{$self->{jobs}};

    if ($index < scalar @{$self->{jobs}}) {
        my $job = $self->{jobs}->[$index];
        if ($job->poll == 0) {
            $index ++;
            
            if ($index < scalar @{$self->{jobs}}) {
                $self->{jobs}->[$index]->submit();
            }
            $self->{index} = $index;
        }
    } 

    if (scalar @{$self->{jobs}} - $index == 0) {
        Plgd::Utils::echo_file($self->get_done_fname(), "0");
    }
    return scalar @{$self->{jobs}}  - $index;
}

1;