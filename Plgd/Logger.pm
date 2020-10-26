package Plgd::Logger;

use strict; 
use warnings;

my $logLevel = 1;


sub currTime() {
    my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime;
    $year += 1900; 
    $mon += 1;
    my $datetime = sprintf ("%d-%02d-%02d %02d:%02d:%02d", $year,$mon,$mday,$hour,$min,$sec);
    return $datetime;
}

sub logMsg($$) {
    my ($type, $msg) = @_;
    my $datetime = currTime();
    print STDERR "$datetime [$type] $msg\n";
}

sub setLevel($) {
    my ($level) = @_;
    if ($level eq "debug") {
        $logLevel = 0;
    } elsif ($level eq "info") {
        $logLevel = 1;
    } elsif ($level eq "warn") {
        $logLevel = 2;
    } elsif ($level eq "error") {
        $logLevel = 3;
    } else {
        error("The log level: $level is not one of (debug, info, warn, error)");
    }
}

sub debug($) {
    logMsg("Debug", $_[0]) if $logLevel <= 0;
}

sub info($) {
    logMsg("Info", $_[0]) if $logLevel <= 1;
}

sub warn($) {
    logMsg("Warning", $_[0]) if $logLevel <= 2;
}

sub error($) {
    logMsg("Error", $_[0]);    # 
    exit(1);
}
 