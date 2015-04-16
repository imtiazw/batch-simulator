#!/usr/bin/env perl
use strict;
use warnings;

use threads;
use threads::shared;
use Thread::Queue;
use Data::Dumper qw(Dumper);

use Trace;
use FCFS;
use FCFSC;
use Backfilling;
use ExecutionProfile ':stooges';
use Random;

local $| = 1;

my ($trace_file_name, $jobs_number, $executions_number, $cpus_number, $cluster_size, $threads_number, $execution_id) = @ARGV;

# This line keeps the code from bein executed if there are uncommited changes in the git tree if the branch being used is the master branch
#my $git_branch = `git symbolic-ref --short HEAD`;
#chomp($git_branch);
#die 'git tree not clean' if ($git_branch eq 'master') and (system('./check_git.sh'));

# Create a directory to store the output
my $basic_file_name = "parser2-$jobs_number-$executions_number-$cpus_number-$execution_id";
mkdir "experiment/parser2/$basic_file_name";

# Basic data for all threads
my @variants = (
	EP_FIRST,
	EP_BEST_EFFORT,
	EP_CONTIGUOUS,
	EP_BEST_EFFORT_LOCALITY,
	EP_CLUSTER,
);

my @variants_names = (
	"backfilling_not_contiguous",
	"backfilling_best_effort",
	"backfilling_contiguous",
	"backfilling_best_effort_locality",
	"backfilling_cluster",
);

# Read the original trace
my $trace = Trace->new_from_swf($trace_file_name);
$trace->remove_large_jobs($cpus_number);
$trace->reset_submit_times();

# Create threads
my $start_time = time();
my @threads;
for my $i (0..($threads_number - 1)) {
	my $thread = threads->create(\&run_all_thread, $i, $execution_id);
	push @threads, $thread;
}

# Wait for all threads to finish
my @results;
push @results, @{$_->join()} for (@threads);

# Print all results in a file
print STDERR "Writing results to experiment/parser2/$basic_file_name/$basic_file_name.csv\n";
write_results_to_file(\@results, "experiment/parser2/$basic_file_name/$basic_file_name.csv");

sub write_results_to_file {
	my ($results, $filename) = @_;

	open(my $filehandle, "> $filename") or die "unable to open $filename";

	print $filehandle join(' ',
		'FIRST_CMAX', 'FIRST_CONTJ', 'FIRST_LOCJ', 'FIRST_LOCF', 'FIRST_RT',
		'BECONT_CMAX', 'BECONT_CONTJ', 'BECONT_LOCJ', 'BECONT_LOCF', 'BECONT_RT',
		'CONT_CMAX', 'CONT_CONTJ', 'CONT_LOCJ', 'CONT_LOCF', 'CONT_RT',
		'BELOC_CMAX', 'BELOC_CONTJ', 'BELOC_LOCJ', 'BELOC_LOCF', 'BELOC_RT',
		'LOC_CMAX', 'LOC_CONTJ', 'LOC_LOCJ', 'LOC_LOCF', 'LOC_RT',
		'TRACE_ID'
	) . "\n";

	for my $results_item (@{$results}) {
		print $filehandle join(' ', @{$results_item}) . "\n";
	}

	close $filehandle;
}

sub run_all_thread {
	my ($id, $execution_id) = @_;
	my @results;

	for my $i (1..($executions_number/$threads_number)) {
		if (!$id) {
			print "Running trace $i/" . $executions_number/$threads_number . "\r";
		}

		my $trace_random = Trace->new_from_trace($trace, $jobs_number);
		$trace_random->write_to_file("experiment/parser2/$basic_file_name/$

		#$trace_random->reset_jobs_numbers();

		my @random_traces = map { Trace->copy_from_trace($trace_random) } (0..$#variants);
		my @schedules = map { Backfilling->new($random_traces[$_], $cpus_number, $cluster_size, $variants[$_]) } (0..$#variants);

		$_->run() for @schedules;

		push @results, [
			(map {
				$_->cmax(),
				$_->contiguous_jobs_number(),
				$_->local_jobs_number(),
				$_->locality_factor(),
				$_->run_time(),
			} @schedules),
			$i
		];
	}

	return [@results];
}

