package Plgd::Config;

use strict; 
use warnings;


sub new {
    my ($cls, $default) = @_;

    my $self = {
        default =>  $default,
        cfg => {},
    };

    bless $self, $cls;
    return $self;
}


# sub load_default($$) {
#     my ($self, $default) = @_;
#     my %cfg = ();
#     for my $i (0 .. @$default){
#         $cfg{@$default[$i][0]} = @$default[$i][1];
#     }
#     return %cfg;
# }

sub load($$) {
    my ($self, $fname);

    $self->{cfg} = loadConfig($fname);
}

sub get($$) {
    my ($self, $name) = @_;
    
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
    
    if (exists($self->{cfg}->{$name0})) {
        return $self->{cfg}->{$name0};
    } elsif (exists($self->{cfg}->{$name1})) {
        return $self->{cfg}->{$name1};
    } else {
        #Plgd::Logger::warn("Not recognizes the config: $name");
        return "";
    }
}

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