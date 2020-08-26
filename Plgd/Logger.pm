package Plgd::Logger;

my $logLevel = "info";

sub setLevel($) {
    my ($level) = @_;

    if ($level == "debug") {
        $logLevel = 0;
    } elsif ($level == "info") {
        $logLevel = 1;
    } elsif ($level == "warn") {
        $logLevel = 2;
    } elsif ($level == "error") {
        $logLevel = 3;
    } else {
        error("The log level: $level is not one of (debug, info, warn, error)");
    }
}



sub currTime() {
    my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime;
    $year += 1900; 
    $mon += 1;
    my $datetime = sprintf ("%d-%02d-%02d %02d:%02d:%02d", $year,$mon,$mday,$hour,$min,$sec);
    return $datetime;
}


sub mylog($$) {
    my ($type, $msg) = @_;
    my $datetime = currTime();
    print STDERR "$datetime [$type] $msg\n";
}


sub debug($) {
    mylog("Debug", @_[0]) if $logLevel <= 0;
}

sub info($) {
    my ($msg) = @_;
    mylog("Info", $msg) if $logLevel <= 1;
}

sub warn($) {
    mylog("Warn", @_[0]) if $logLevel <= 2;
}

sub error($) {
    mylog("Error", @_[0]);    # 
    exit(1);
}
 

1;