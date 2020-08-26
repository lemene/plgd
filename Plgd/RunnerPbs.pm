package Plgd::RunnerPbs;


use Cwd;
use File::Basename;

use Plgd::Runner;
use Plgd::Utils;

use strict;

our @ISA = qw(Plgd::Runner);


sub new { 
    my ($class) = @_;
    my $self = $class->SUPER::new(); 
    detect($self);
    bless $self, $class; 
    return $self; 
} 

sub valid($) {
    my ($self) = @_;
    return not $self->{path} eq "";
}

our $isPro = "";
our $version = "";
our $VERSION = '1.00';

sub detect($) {
    my ($self) = @_;

    my $path = `which pbsnodes 2> /dev/null`;
    $self->{path} = trim($path);

    if (not $self->{path} eq "") {

        open(F, "pbsnodes --version 2>&1 |");
        while (<F>) {
            if (m/pbs_version\s+=\s+(.*)/) {
                $self->{isPro}   =  1;
                $self->{version} = $1;
            }
            if (m/Version:\s+(.*)/) {
                $self->{version} = $1;
            }
        }
        close(F);
    
        if ($isPro == 0) {
            Plgd::Logger::info("Found PBS/Torque '$version', which is $path");
            return "PBS";
        } else {
            Plgd::Logger::info("Found PBS/Pro '$version', which is $path");
            return "PBS";
        }

    } else {
        return undef;
    } 
}



sub submitScript($$$$) {
    
    my ($script, $thread, $memory, $options) = @_;

    my $jobName = basename($script);

    my $cmd = "qsub -j oe";
    $cmd = $cmd . " -d `pwd`" if ($isPro == 0); 
    $cmd = $cmd . " -N $jobName";                               # name
    $cmd = $cmd . " -l nodes=1:ppn=$thread" if ($thread > 0);   # thread
    $cmd = $cmd . " -l mem=$memory" if ($memory > 0);           # memory
    $cmd = $cmd . " -o $script.log";                            # output
    $cmd = $cmd . " $options";                                  # other options
    $cmd = $cmd . " $script";                                   # script
    Plgd::Logger::info("Sumbit command: $cmd");    
    my $result = `$cmd`;

    if (not $result eq "") {
        return trim($result);
    } else {
        Plgd::Logger::info("Failed to sumbit command");
    }
}

sub stopScriptPbs($) {
    my ($job) = @_;
    my $cmd = "qdel $job";
    Plgd::Logger::info("Stop script: $cmd");
    `$cmd`;
}

sub checkScriptPbs($$) {
    my ($script, $jobid) = @_;
    my $state = "";
    open(F, "qstat |");
    while (<F>) {
        my @items = split(" ", $_);
        if (scalar @items >= 6 and $jobid =~ /$items[0]/) {
            $state = $items[4];
            break;
        }
        
    }
    close(F);
    return $state;
} 



