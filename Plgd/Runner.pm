package Plgd::Runner;

use Plgd::RunnerLocal;
use Plgd::RunnerPbs;

sub new {
    my $class = shift;
    my $self = {
        _test => shift,
    };
    bless($self, $class);
    printf("bbb\n");
    return $self;
}

sub valid($) {
    my ($self) = @_;
    return 1;
}

sub create($$) {
    my ($type, $node) = @_;

    if ($type eq "pbs") {
        return Plgd::RunnerPbs->new();
    } elsif ($type eq "local") {
        return Plgd::RunnerLocal->new();

    }
}
1;
