package Plgd::Utils;

require Exporter;

@ISA = qw(Exporter);
@EXPORT = qw(linesInFile filesNewer trim echoFile requireFiles waitRequiredFiles mergeOptionString plgdLogLevel Plgd::Logger::debug Plgd::Logger::info Plgd::Logger::warn Plgd::Logger::error getFileFirstItem wrapCmdWithPreCheck deleteFiles);

use File::Path;
use strict; 

our $logLevel = 1;

sub trim { 
    my $s = shift; 
    $s =~ s/^\s+|\s+$//g; 
    return $s 
}

sub deleteFiles {
    foreach my $p (@_) {
        my @items = glob($p);
        foreach my $i (@items) {
            if (-f $i) {
                unlink($i);
            } else {
                rmtree($i);
            }
        }
    }
}

sub linesInFile($) {
    my ($fname) = @_;
    my @lines = ();
    open(F, "<$fname") or die "cann't open file: $fname, $!";
    while(<F>) {
        if ($_) {
            my $line = $_;
            chomp($line); 
            if ($line eq "") {
                next;
            }
            push @lines, $line;
            
        }
    }
    close(F);
    return @lines;
}

## echo's function
## For effect of 'echo -e' is inconsistent on different platforms.
sub echoFile($$) {
    my ($fname, $msg) = @_;
    
    open(F, "> $fname") or die; 
    print F ($msg);
    close(F);
}

sub filesNewer($$) {
    my ($files1, $files2) = @_;

    my $tm = 0;
    
    return 1 if ((scalar @$files1 == 0 ) or (scalar @$files2 == 0));

    foreach my $f (@$files1) {
        if ((-e $f) and (stat($f))[9] > $tm) {
            $tm = (stat($f))[9];
        }
    }

    foreach my $f (@$files2) {
        if (not -e $f) {return 1; }
        if ((stat($f))[9] < $tm) { return 1;}
    }
    return 0;
}

sub stringToOptions($) {
    my ($str) = @_;
    my %opts = ();
    my @items = split(" ", $str);

    for (my $i = 0; $i+1 < scalar @items; $i = $i+2) {
       $opts{$items[$i]} = $items[$i+1];
    }
    return %opts
}

sub optionsToString($) {
    my ($opts) = @_;
    my $str = "";

    while (my ($k,$v) = each %$opts ) {
       $str = $str . " $k $v";
    }
    return $str;
}

sub mergeOptionString($$) {
    my ($str1, $str2) = @_;
    my %opt = (stringToOptions($str1), stringToOptions($str2));
    my $str = optionsToString(\%opt);
    return $str;
}


sub getFileFirstItem($$) {
    my ($file, $line) = @_;
    my $i = 0;
    open(F, "< $file") or die; 
    while(<F>) {
        if ($i == $line) {
           my @items = split(" ", $_);
           close(F);
           return $items[0];
        }
        $i = $i + 1;
    }
    close(F);
    die;
}


sub waitRequiredFiles {
    my ($waitingTime, @files) = @_;

    my $startTime = time();
    my $sleepTime = 1;
    while ( 1 ) {
        my $finished = 0;
        my $notExist = "";
        foreach my $f (@files) {
            if (-e $f) {
                $finished += $finished + 1;
            } else {
                $notExist = $f;
                last;
            }
              
        }
        
        if ($finished < scalar @files) {
            if (time() - $startTime <= $waitingTime) {
                sleep($sleepTime);
            } else {
                Plgd::Logger::error("File is not exist: $notExist");
            }
        } else {
            last;
        }
    }
}


sub requireFiles {
    foreach my $f (@_) {
        Plgd::Logger::debug("Require file, $f");
        if (not -e $f) {
            Plgd::Logger::error("File is not exist: $f");
        }
    }
}

1;