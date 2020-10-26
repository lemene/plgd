package Plgd::Config;

use strict; 
use warnings;


sub trim { 
    my $s = shift; 
    $s =~ s/^\s+|\s+$//g; 
    return $s 
}


sub loadConfig($) {
    my ($fname) = @_;
    my %cfg = ();

    open(F, "<$fname") or die "cann't open file: $fname, $!";
    while(<F>) {
        my @items  = split("=", $_, 2);
        $items[1] =~s/^\s*"|"\s*$//g;
        $cfg{$items[0]} = trim($items[1]);
    }

    return \%cfg;
}


sub switchRunningConfig($$) {
    my ($cfg, $prefix) = @_;

    my $switch = sub ($$$$) {
        my ($cfg, $old, $name) = @_;
        if (exists($cfg->{$prefix . "_" . $name}) and $cfg->{$prefix . "_" . $name} ne "") {
           $old->{$name} = $cfg->{$name};
           $cfg->{$name} = $cfg->{$prefix . "_" . $name};
        }
    };

    my $old = {};
    $switch->($cfg, $old, "MEMORY");
    $switch->($cfg, $old, "THREADS");
    return $old;
}

    
sub resumeConfig($$) {
    my ($cfg, $old) = @_;

    foreach my $k (keys %$old) {
        $cfg->{$k} = $old->{$k};
    }
}

1;