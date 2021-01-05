package Plgd::Utils;

require Exporter;

@ISA = qw(Exporter);
@EXPORT = qw(linesInFile filesNewer trim echoFile require_files waitRequiredFiles mergeOptionString plgdLogLevel wrapCmdWithPreCheck deleteFiles);

use File::Path;
use strict; 

use Plgd::Logger;

sub trim { 
    my $s = shift; 
    $s =~ s/^\s+|\s+$//g; 
    return $s 
}



sub fileLength($) {
    my ($fname) = @_;
    my @args = stat ($fname);
    return $args[7];
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
sub echo_file($$) {
    my ($fname, $msg) = @_;
    
    open(F, "> $fname") or die; 
    print F ($msg);
    close(F);
}

sub file_exist($) {
    my ($files) = @_;

    foreach my $f (@$files) {
        if (not -e $f) {return 0; }
    }

    return 1;
}

sub file_latest_mtime($) {
    my ($files) = @_;

    my $tm = 0;
    foreach my $f (@$files) {
        if ((stat($f))[9] > $tm) {
            $tm = (stat($f))[9];
        }
    }
    return $tm;
}

sub file_earliest_mtime($) {
    my ($files) = @_;

    my $tm = -1;
    foreach my $f (@$files) {
        if ($tm < 0 or (stat($f))[9] < $tm) {
            $tm = (stat($f))[9];
        }
    }
    return $tm;

}

sub file_newer($$) {
    my ($files1, $files2) = @_;

    return file_earliest_mtime($files1) > file_latest_mtime($files2);
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


sub require_files {
    foreach my $f (@_) {
        Plgd::Logger::debug("Require file, $f");
        if (not -e $f) {
            Plgd::Logger::error("File is not exist: $f");
        }
    }
}
