package RaconProject;

use BioUtils;

use Plgd::Project;
our @ISA = qw(Plgd::Project);  

my @defaultConfig = (
    ["PROJECT", ""],
    ["READS", ""],
    ["CONTIGS", ""],
    ["THREADS", "4"],
    ["CLEANUP", "0"],
    ["GRID_NODE", "0"],
    ["READ_BLOCK_SIZE", "4000000000"],
    ["CONTIG_BLOCK_SIZE", "500000000"],
    ["ITERATION_NUMBER", 1],
    ["MINIMAP2_OPTIONS", "-x map-pb"],
    ["RACON_OPTIOINS", ""]
);

sub new {
    my ($class, @params) = @_;
     
    my $self = $class->SUPER::new(); 
    $self->{defaultConfig} = \@defaultConfig;
    bless($self, $class);
    printf("new RaconProject\n");
    return $self;
}

sub initialize {
    my ($self, $cfgfile) = @_;

    $self->SUPER::initialize($cfgfile);
    print $self->{cfg}{PRJECT};
    
    my @required = ("PROJECT", "CONTIGS", "READS");
    foreach my $r (@required) {
        if (not exists($self->{cfg}{$r}) or $self->{cfg}{$r} eq "")  {
            Plgd::Logger::error("Not set config $r");
        }
    }
    return %cfg;
}

sub polish($$$) {
    my ($self, $cfgfile) = @_;

    $self->initialize($cfgfile);
    my $cfg = $self->{cfg};
    my $env = $self->{env};

    my $prjDir = $env->{"WorkPath"} ."/". $cfg->{"PROJECT"};
    my $count = $cfg->{"ITERATION_NUMBER"};
    my $finalPolished = "$prjDir/polished.fasta";

    my $contigs = $cfg->{"CONTIGS"};
    my $reads = $cfg->{"READS"};
    my $polished = "";
    printf("contigs: $contigs\n");
    mkdir($prjDir);
    mkdir($prjDir . "/scripts");

    for (my $i=0; $i<$count; $i=$i+1) {
        $polished = "$prjDir/iter_$i/polished.fasta";
        my $job = jobPolishWithMinimap2Racon($env, $cfg, "iter_$i", $contigs, $reads, $polished, 
            [$cfg->{"MINIMAP2_OPTIONS"}, $cfg->{"RACON_OPTIONS"}],
            [$cfg->{"READ_BLOCK_SIZE"}, $cfg->{"CONTIG_BLOCK_SIZE"}], "$prjDir/iter_$i");
        serialRunJobs($env, $cfg, $job); 
        $contigs = $polished;
    }    


}

sub usage() {
    my ($self) = @_;
    
    print "Usage: racon.pl config|polish cfgname\n".
          "    polish:      polish contigs\n" .
          "    config:      generate default config file\n" 

}

sub run ($$$) {
    my ($self, $cmd, $cfgfile) = @_;
    
    if ($cmd eq "polish") {
        $self->polish($cfgfile);
    } elsif ($cmd eq "config") {
        $self->writeDefaultConfig($cfgfile);
    } else {
        $self->usage();
    }
    #     if (scalar @ARGV >= 2) {
    #     my $cmd = @ARGV[1];
    #     my $cfgfname = @ARGV[2];

    # } else {
    #     $prj->usage();
    # }
}

1;