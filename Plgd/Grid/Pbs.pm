package Plgd::Grid::Pbs;

our @ISA = qw(Plgd::Grid);   # inherits from Grid 

use Cwd;
use File::Basename;

use Plgd::Utils;

sub new($) {
    my ($cls) = @_;

    my $path = `which pbsnodes 2> /dev/null`;
    $path = trim($path);

    if (not $path eq "") {

        my $isPro = "";
        my $version = "";

        open(F, "pbsnodes --version 2>&1 |");
        while (<F>) {
            if (m/pbs_version\s+=\s+(.*)/) {
                $isPro   =  1;
                $version = $1;
            }
            if (m/Version:\s+(.*)/) {
                $version = $1;
            }
        }
        close(F);
        
        my $self = $cls->SUPER::new(); 
        $self->{name} = "slurm";
        $self->{path} = $path;
        $self->{version} = $version;
        $self->{isPro} = $isPro;

        bless $self, $cls;
        return $self;

    } else {
        return undef;
    } 
}


sub submitScript($$$$) {
    
    my ($self, $script, $thread, $memory, $options) = @_;

    my $isPro = $self->{is_pro};

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

sub stopScriptPbs($$) {
    my ($self, $job) = @_;
    my $cmd = "qdel $job";
    Plgd::Logger::info("Stop script: $cmd");
    `$cmd`;
}

sub checkScriptPbs($$$) {
    my ($self, $script, $jobid) = @_;
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



