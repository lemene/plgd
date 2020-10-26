package Plgd::Cluster;

use strict; 
use warnings;

use Plgd::ClusterLocal;
use Plgd::ClusterPbs;
use Plgd::ClusterSge;
use Plgd::ClusterLsf;
use Plgd::ClusterSlurm;

sub new {
    my ($cls) = @_;

    my $self = {

    };

    bless $self, $cls;
    return $self;
}

sub create($$) {
    my ($cls, $cluster) = @_;       # cluster is "pbs:4"

    printf("ccc $cluster ccc\n");
    my @items = split(":", $cluster);
    my $type = scalar @items >= 1 ? trim($items[0]) : "auto";
    my $nodes = scalar @items >= 2 ? $items[1] + 0 : "0";
    my $runner = undef;

    if ($type eq "auto") {
        $runner = Plgd::ClusterSlurm->create() if (not $runner);
        $runner = Plgd::ClusterPbs->create() if (not $runner);
        $runner = Plgd::ClusterLsf->create() if (not $runner);
        $runner = Plgd::ClusterSge->create() if (not $runner);
        $runner = Plgd::ClusterLocal->create() if (not $runner);
    } elsif ($type eq "pbs") {
        $runner = Plgd::ClusterPbs->create() if (not $runner);
    } elsif ($type eq "lsf") {
        $runner = Plgd::ClusterLsf->create() if (not $runner);
    } elsif ($type eq "sge") {
        $runner = Plgd::ClusterSge->create() if (not $runner);
    } elsif ($type eq "slurm") {
        $runner = Plgd::ClusterSlurm->create() if (not $runner);
    } elsif ($type eq "local") {
        $runner = Plgd::ClusterLocal->create();
    } else {
        Plgd::Logger::error("Not support cluster:  $type");
    }
    
    if (not $runner) {
        Plgd::Logger::error("Not support cluster:  $cluster");
    }

    return $runner;
}


sub submit() {

}



1;