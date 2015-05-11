package FCFSC;
use parent 'Schedule';

use strict;
use warnings;
use List::Util qw(max reduce);
use Data::Dumper qw(Dumper);

sub compute_block {
	my ($self, $first_processor_id, $requested_cpus) = @_;
	my @selected_processors;

	for my $index ($first_processor_id..($first_processor_id+$requested_cpus-1)) {
		my $real_index = $index % @{$self->{processors}};
		push @selected_processors, $self->{processors}->[$real_index];
	}
	my $starting_time = max map {$_->cmax()} @selected_processors;

	return {
		starting_time => $starting_time,
		selected_processors => [@selected_processors]
	};
}

sub assign_job {
	my ($self, $job) = @_;
	my $requested_cpus = $job->requested_cpus;
	die "not enough processors (we need $requested_cpus, we have $self->{num_processors})" if $requested_cpus > $self->{num_processors};

	my @available_blocks = map {$self->compute_block($_, $requested_cpus)} (0..$self->{num_processors});
	my $best_block;

	for my $block (@available_blocks) {
		my $block_starting_time = $block->{starting_time};
		$best_block = $block if not defined $best_block or $block_starting_time < $best_block->{starting_time};
	}

	$job->assign_to(max($job->submit_time(), $best_block->{starting_time}), $best_block->{selected_processors});
}

1;
