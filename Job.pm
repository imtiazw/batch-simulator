package Job;

use strict;
use warnings;

use List::Util qw(min max);
use POSIX;
use Log::Log4perl qw(get_logger);
use Carp;
use Data::Dumper;

use overload '""' => \&stringification;

my @svg_colors = (
	'orange',
	'green',
	undef,
	undef,
	undef,
	'red',
);

my @svg_colors_platform = (
	'blue',
	'green',
	'yellow',
	'orange',
	'red',
	'purple',
	'brown',
);

use Exporter;
our @ISA = qw(Exporter);

use constant {
	JOB_STATUS_COMPLETED => 1,
	JOB_STATUS_FAILED => 0,
	JOB_STATUS_CANCELED => 5,
};

our @EXPORT = qw(JOB_STATUS_COMPLETED JOB_STATUS_FAILED JOB_STATUS_CANCELED);

sub stringification {
	my $self = shift;

	return join(' ',
		map {(defined $_) ? $_ : '?'} (
			$self->{job_number},
			$self->{submit_time},
			$self->wait_time(),
			$self->{run_time},
			$self->{allocated_cpus},
			$self->{avg_cpu_time},
			$self->{used_mem},
			$self->{requested_cpus},
			$self->{requested_time},
			$self->{requested_mem},
			$self->{status},
			$self->{uid},
			$self->{gid},
			$self->{exec_number},
			$self->{queue_number},
			$self->{partition_number},
			$self->{prec_job_number},
			$self->{think_time_prec_job}
		)
	);
}

sub new {
	my $class = shift;
	my $logger = get_logger('Job::new');

	my $self = {
		job_number => shift, #1
		submit_time => shift, #2
		original_wait_time => shift, #3
		run_time => shift, #4
		allocated_cpus => shift, #5
		avg_cpu_time => shift, #6
		used_mem => shift, #7
		requested_cpus => shift, #8
		requested_time => shift, #9
		requested_mem => shift, #10
		status => shift, #11, 0 = failed, 5 = cancelled, 1 = completed
		uid => shift, #12
		gid => shift, #13
		exec_number => shift, #14
		queue_number => shift, #15
		partition_number => shift, #16
		prec_job_number => shift, #17
		think_time_prec_job => shift, #18
	};

	# Sanity checks for some of the job fields
	#$logger->logdie('job run time is 0') if ($self->{run_time} == );
	$self->{status} = 5 if (($self->{run_time} == 0) and ($self->{status} == 1));

	#$logger->logdie('number of allocated CPUs is different than requested') if ($self->{allocated_cpus} != $self->{requested_cpus});
	$self->{allocated_cpus} = $self->{requested_cpus} if ($self->{allocated_cpus} != $self->{requested_cpus});

	#$logger->logdie('requested time smaller than run time') if ($self->{requested_time} < $self->{run_time});
	$self->{run_time} = $self->{requested_time} if ($self->{requested_time} < $self->{run_time});

	# Reset the job status for now. Maybe later we will do something
	# different with this
	$self->{status} = JOB_STATUS_COMPLETED;

	bless $self, $class;
	return $self;
}

sub copy {
	my $class = shift;
	my $original = shift;

	my $self = {};
	%{$self} = %{$original};

	bless $self, $class;
	return $self;
}

sub schedule_time {
	my $self = shift;
	my $schedule_time = shift;

	$self->{schedule_time} = $schedule_time if defined $schedule_time;

	return $self->{schedule_time};
}

sub requested_cpus {
	my $self = shift;
	return $self->{requested_cpus};
}

sub run_time {
	my $self = shift;
	my $run_time = shift;

	$self->{run_time} = $run_time if (defined $run_time);

	return $self->{run_time};
}

sub requested_time {
	my $self = shift;
	my $requested_time = shift;

	$self->{requested_time} = $requested_time if (defined $requested_time);

	return $self->{requested_time};
}

sub starting_time {
	my $self = shift;
	my $starting_time = shift;

	$self->{starting_time} = $starting_time if defined $starting_time;

	return $self->{starting_time};
}

sub starts_after {
	my $self = shift;
	my $time = shift;
	my $logger = get_logger('Job::starts_after');

	$logger->logdie('undefined job starting time') unless defined $self->{starting_time};

	return ($self->{starting_time} > $time);
}

sub real_ending_time {
	my $self = shift;
	my $logger = get_logger('Job::real_ending_time');

	return unless defined $self->{starting_time};
	return $self->{starting_time} + $self->{run_time};
}

sub submitted_ending_time {
	my $self = shift;
	my $logger = get_logger('Job::submitted_ending_time');

	$logger->logdie('undefined job starting time') unless defined $self->{starting_time};

	return $self->{starting_time} + $self->{requested_time};
}

sub flow_time {
	my $self = shift;
	my $logger = get_logger('Job::flow_time');

	$logger->logdie('undefined job starting time') unless defined $self->{starting_time};

	return $self->{starting_time} + $self->{run_time} - $self->{submit_time};
}

sub bounded_stretch {
	my $self = shift;
	my $time_limit = shift;

	return  unless defined $self->wait_time() and defined $self->{run_time};
	return max(($self->wait_time() + $self->{run_time})/max($self->{run_time}, ((defined $time_limit) ? $time_limit : 10)), 1);
}

sub bounded_stretch_with_cpus_squared {
	my $self = shift;
	my $time_limit = shift;

	$time_limit = 10 unless (defined $time_limit);
	return max(($self->wait_time() + $self->{run_time}) / (max($self->{run_time}, $time_limit) * sqrt($self->{allocated_cpus})), 1);
}

sub bounded_stretch_with_cpus_log {
	my $self = shift;
	my $time_limit = shift;

	$time_limit = 10 unless (defined $time_limit);
	return max(($self->wait_time() + $self->{run_time})
		/ (max($self->{run_time}, $time_limit)
		* ($self->{allocated_cpus} == 1) ? 1 : log($self->{allocated_cpus})), 1);
}

sub original_bounded_stretch {
	my $self = shift;
	my $time_limit = shift;

	my $logger = get_logger('Job::original_bounded_stretch');

	$logger->logconfess('undefined job parameters') unless defined $self->{original_wait_time} and defined $self->{run_time};
	return max(($self->{original_wait_time} + $self->{run_time})/max($self->{run_time}, ((defined $time_limit) ? $time_limit : 10)), 1);
}

sub stretch {
	my $self = shift;
	return $self->wait_time()/$self->{run_time};
}

sub submit_time {
	my $self = shift;
	my $submit_time = shift;

	$self->{submit_time} = $submit_time if defined $submit_time;
	return $self->{submit_time};
}

sub original_wait_time {
	my $self = shift;

	return $self->{original_wait_time};
}

sub wait_time {
	my $self = shift;
	my $logger = get_logger('Job::wait_time');

	return unless defined $self->{submit_time} and defined $self->{starting_time};
	return $self->{starting_time} - $self->{submit_time};
}

sub job_number {
	my $self = shift;
	my $job_number = shift;

	$self->{job_number} = $job_number if defined $job_number;

	return $self->{job_number};
}

sub status {
	my $self = shift;
	my $status = shift;

	$self->{status} = $status if (defined $status);
	return $self->{status};
}

sub unassign {
	my $self = shift;

	delete $self->{starting_time};

	if (defined $self->{assigned_processors_ids}) {
		$self->{assigned_processors_ids}->free_allocated_memory();
		delete $self->{assigned_processors_ids};
	}

	return;
}

sub assign {
	my $self = shift;
	my $starting_time = shift;
	my $assigned_processors = shift;

	$self->{starting_time} = $starting_time;
	$self->{assigned_processors_ids}->free_allocated_memory() if defined $self->{assigned_processors_ids};
	$self->{assigned_processors_ids} = $assigned_processors;

	return;
}

sub assigned_processors_ids {
	my $self = shift;
	return $self->{assigned_processors_ids};
}

sub svg {
	my $self = shift;
	my $fh = shift;
	my $w_ratio = shift;
	my $h_ratio = shift;
	my $current_time = shift;
	my $platform = shift;

	my $used_clusters = $self->{assigned_processors_ids}->list_of_used_clusters($platform->cluster_size());
	my $job_platform_level = $platform->job_level_distance($used_clusters);

	$self->{assigned_processors_ids}->ranges_loop(
		sub {
			my ($start, $end) = @_;
			die "$start is after $end" if $end < $start;
			#rectangle
			my $x = $self->{starting_time} * $w_ratio;
			my $w;
			if ($self->real_ending_time() <= $current_time) {
				$w = $self->{run_time} * $w_ratio;
			} else {
				$w = $self->{requested_time} * $w_ratio;
			}

			my $y = $start * $h_ratio;
			my $h = $h_ratio * ($end - $start + 1);
			#my $color = $svg_colors[$self->{status}];
			my $color = $svg_colors_platform[$job_platform_level];
			my $sw = min($w_ratio, $h_ratio) / 10;
			if ($self->real_ending_time() > $current_time) {
				my $x = ($self->{starting_time}+$self->{run_time}) * $w_ratio;
				my $w = ($self->{requested_time}-$self->{run_time}) * $w_ratio;
				print $fh "\t<rect x=\"$x\" y=\"$y\" width=\"$w\" height=\"$h\" style=\"fill:black;fill-opacity:1.0\"/>\n";
			}
			print $fh "\t<rect x=\"$x\" y=\"$y\" width=\"$w\" height=\"$h\" style=\"fill:$color;fill-opacity:0.2;stroke:black;stroke-width:$sw\"/>\n";
			#label
			$x = $w/2 + $self->{starting_time} * $w_ratio;
			$y = (($start+$end+1)/2) * $h_ratio;
			my $fs = min($h_ratio*($end-$start+1), $w/5);
			die "negative font size :$fs ; $end ; $start $h_ratio $w $self->{run_time}" if $fs <= 0;
			my $text_y = $y + $fs*0.35;
			print $fh "\t<text x=\"$x\" y=\"$text_y\" fill=\"black\" font-family=\"Verdana\" text-anchor=\"middle\" font-size=\"$fs\">$self->{job_number}-$job_platform_level</text>\n";
		}
	);
	return;
}

sub used_clusters {
	my $self = shift;
	my $cluster_size = shift;

	return $self->{assigned_processors_ids}->used_clusters($cluster_size);
}

sub list_of_used_clusters {
	my $self = shift;
	my $cluster_size = shift;

	return $self->{assigned_processors_ids}->list_of_used_clusters($cluster_size);
}

sub clusters_required {
	my $self = shift;
	my $cluster_size = shift;

	return POSIX::ceil($self->requested_cpus() / $cluster_size);
}

sub DESTROY {
	my $self = shift;

	$self->{assigned_processors_ids}->free_allocated_memory()
		if defined $self->{assigned_processors_ids};

	return;
}

1;
