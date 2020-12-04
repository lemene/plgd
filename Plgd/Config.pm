package Plgd::Config;

use strict; 
use warnings;


sub _init_defcfg($) {
    my ($default) = @_;
    my %cfg = ();

    for my $i (0 .. $#{$default}){
        $cfg{lc($default->[$i][0])} = $default->[$i][1];
    }
    return \%cfg;
}

# default format is (name, value, required, desc)
sub new {
    my ($cls, $default) = @_;
    my $self = {
        default => $default,
        cfg => {},
        defcfg => _init_defcfg($default)
    };
    bless $self, $cls;
    return $self;
}

sub load($$) {
    my ($self, $fname) = @_;

    open(F, "<$fname") or die "cann't open file: $fname, $!";
    while(<F>) {
        my $line = Plgd::Utils::trim($_);
        if ($line ne "" and not $line =~ m/^#/) {
            my @items  = split("=", $line, 2);
            if (scalar @items == 2) {
                my $n = lc(Plgd::Utils::trim($items[0]));
                my $v = Plgd::Utils::trim($items[1]);
                $self->{cfg}->{$n} = $v;
            } else {
                Plgd::Logger::error("Unrecogined Config line: $line");
            }
        }
    }
    close(F);

    $self->check($self->{cfg});

}

sub check($$) {
    my ($self, $cfg) = @_;

    # check required
    for my $i (@{$self->{default}}) {
        if ($i->[2]) {
            my $n = lc($i->[0]);
            if (not exists($cfg->{$n}) or $cfg->{$n} eq "") {
                Plgd::Logger::error("Not set config $i->[0]");
            }
        }
    }
}

sub get($$) {
    my ($self, $name) = @_;

    $name = lc($name);

    if (exists($self->{cfg}->{$name})) {
        return $self->{cfg}->{$name};
    } elsif (exists($self->{defcfg}->{$name})) {
        return $self->{defcfg}->{$name};
    } else {
        #Plgd::Logger::warn("Not recognizes the config: $name");
        return "";
    }
}
sub get2($$$) {
    my ($self, $name0, $name1) = @_;
    $name0 = lc($name0);
    $name1 = lc($name1);
    
    if (exists($self->{cfg}->{$name0})) {
        return $self->{cfg}->{$name0};
    } elsif (exists($self->{cfg}->{$name1})) {
        return $self->{cfg}->{$name1};
    } else {
        #Plgd::Logger::warn("Not recognizes the config: $name");
        return "";
    }
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