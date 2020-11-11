package Plgd::Job;

use strict; 
use warnings;

use Plgd::Job::Script;
use Plgd::Job::Function;
use Plgd::Job::Serial;
use Plgd::Job::Parallel;


my $WAITING_FILE_TIME = 3;

sub create($$$) {
    my ($cls, $pl, %params) = @_;

    if (exists $params{cmds} and scalar $params{cmds} > 0) {      # 
        return Plgd::Job::Script->new($pl, %params);
    } elsif (scalar $params{jobs}) {
        return Plgd::Job::Serial->new($pl, %params);
    } elsif (scalar $params{pjobs}) {
        return Plgd::Job::Parallel->new($pl, %params);
    } elsif (scalar $params{funcs}) {
        return Plgd::Job::Function->new($pl, %params);
    } else {
        return undef;
    }
}

sub new() {
    my ($cls, $pl, %params) = @_;

    printf("ssss\n");    
    my $self = {
        pl => $pl,
        name => $params{name},
        ifiles => $params{ifiles},
        ofiles => $params{ofiles},
        gfiles => $params{gfiles},
        mfiles => $params{mfiles},
        msg => $params{msg},
        prefunc => $params{prefunc},
        postfunc => $params{postfunc}
    };

    bless $self, $cls;
    return $self;
}

sub get_script_fname($) {
    my ($self) = @_;

    return $self->{pl}->get_script_fname($self->{name});
}

sub get_done_fname() {
    my ($self) = @_;
    return $self->get_script_fname() . ".done";
}

sub is_succ_done($) {
    my ($self) = @_;

    my $script = $self->get_script_fname();
    return Plgd::Utils::filesNewer($self->{ofiles}, $self->{ifiles}) and Plgd::Script::isScriptSucc($script);
}

sub preprocess($) {
    my ($self) = @_;

    Plgd::Logger::info("Start running job $self->{name}, $self->{msg}");
    $self->{prefunc}->($self) if ($self->{prefunc});
    
    my $script = $self->get_script_fname();
    Plgd::Utils::requireFiles(@{$self->{ifiles}});
    if (not $self->is_succ_done()) {
        Plgd::Utils::deleteFiles(@{$self->{gfiles}}) if ($self->{gfiles}); 
        Plgd::Utils::deleteFiles($self->get_done_fname()); 

        #Plgd::Logger::info("Start " . $self->{msg} . ".") if ($self->{msg});
        return 0;
    }
    return 1;
}

sub postprocess($$) {
    my ($self, $skipped) = @_;

    if (not $skipped) {

        Plgd::Utils::waitRequiredFiles($WAITING_FILE_TIME, @{$self->{ofiles}});
        
        Plgd::Utils::deleteFiles(@{$self->{mfiles}}); # 是否需要删除临时文件
        
        Plgd::Logger::info("End " .$self->{msg} . ".") if ($self->{msg});
    } else {
        Plgd::Logger::info("Skip ". $self->{msg} . " for outputs are newer.") if ($self->{msg});
    }
    $self->postfunc->($self) if ($self->{postfunc});
}


sub run($) {
    my ($self) = @_;

    my $skipped = $self->preprocess();

    if (not $skipped) {
        $self->run_core();
    }
    $self->postprocess($skipped);

}

1;
