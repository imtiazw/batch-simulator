#!/usr/bin/env perl
use strict;
use warnings;

use Data::Dumper qw(Dumper);
use threads;
use threads::shared;
use Thread::Queue;
use Log::Log4perl qw(get_logger);
use Time::HiRes qw(time);

use Trace;
use Backfilling;
use ExecutionProfile ':stooges';

my $trace_file = '../swf/CEA-Curie-2011-2.1-cln-b1-clean2.swf';
my $schedule_script = './run_schedule.pl';
my $experiment_path = 'experiment/run_instances_paper';
my $execution_id = 2;
my $instances = 512;
my $jobs_number = 300;
my $cpus_number = 512;
my $cluster_size = 16;
my $threads_number = 6;
#my @backfilling_variants = (EP_FIRST, EP_BEST_EFFORT, EP_CONTIGUOUS, EP_BEST_EFFORT_LOCALITY, EP_CLUSTER);
my @backfilling_variants = (EP_FIRST);

$SIG{INT} = \&catch_signal;

my $results = [];
share($results);

Log::Log4perl::init('log4perl.conf');
my $logger = get_logger('test_time');

$logger->info("Reading trace");
my $trace = Trace->new_from_swf($trace_file);
$trace->remove_large_jobs($cpus_number);
$trace->reset_submit_times();
$trace->reset_jobs_numbers();

# Create a directory to store the output
my $basic_file_name = "run_instances-$jobs_number-$instances-$cpus_number-$execution_id";
my $experiment_folder = "$experiment_path/$basic_file_name";
mkdir $experiment_folder unless (-d $experiment_folder);

$logger->info("Creating queue\n");
my $q = Thread::Queue->new();
$q->enqueue($_) for (0..($instances - 1));
$q->end();

my $run_time = time();

$logger->info("Creating threads");
my @threads = map {threads->create(\&run_instance, $_)} (0..($threads_number - 1));

$logger->info("Waiting for threads to finish");
while ((my $running_threads = threads->list()) > 0) {
	my @joinable_threads = threads->list(threads::joinable);
	$_->join() for (@joinable_threads);
	sleep(5);
}

$logger->info("Writing results to file $experiment_folder/$basic_file_name.csv");
write_results_to_file();

$logger->info("Done");

sub run_instance {
	my $id = shift;
	my $logger = get_logger('test_time::run_instance');

	# Exit the thread if a signal is received
	$SIG{INT} = sub { $logger->info("Killing thread $id"); threads->exit(); };

	while (defined(my $instance = $q->dequeue_nb())) {
		$logger->info("Thread $id running $instance");

		my $trace_instance_file = "$experiment_folder/$instance.swf";
		$logger->logdie("trace file $trace_instance_file doesn't exist") unless (-f $trace_instance_file);

		my $results_instance = [];
		share($results_instance);

		for my $backfilling_variant (@backfilling_variants) {
			my $schedule_thread = threads->create(\&run_schedule, $trace_instance_file, $backfilling_variant);
			my $schedule_result = $schedule_thread->join();

			push @{$results_instance}, @$schedule_result;
		}

		push @{$results_instance}, $instance;
		$results->[$instance] = $results_instance;
	}

	$logger->info("Thread $id finished");
	return;
}

sub run_schedule {
	my $trace_file = shift;
	my $backfilling_variant = shift;

	my $schedule_result = `$schedule_script $trace_file $cpus_number $cluster_size $backfilling_variant`;
	my ($cmax, $contiguous_jobs_number, $local_jobs_number, $locality_factor, $run_time) = split(' ', $schedule_result);

	return [$cmax, $contiguous_jobs_number, $local_jobs_number, $locality_factor, $run_time];
}

sub write_results_to_file {
	open (my $file, '>', "$experiment_folder/$basic_file_name.csv") or die "unable to open $experiment_folder/$basic_file_name";

	print $file join(' ',
		'FIRST_CMAX', 'FIRST_CONTJ', 'FIRST_LOCJ', 'FIRST_LOCF', 'FIRST_RT',
		'BECONT_CMAX', 'BECONT_CONTJ', 'BECONT_LOCJ', 'BECONT_LOCF', 'BECONT_RT',
		'CONT_CMAX', 'CONT_CONTJ', 'CONT_LOCJ', 'CONT_LOCF', 'CONT_RT',
		'BELOC_CMAX', 'BELOC_CONTJ', 'BELOC_LOCJ', 'BELOC_LOCF', 'BELOC_RT',
		'LOC_CMAX', 'LOC_CONTJ', 'LOC_LOCJ', 'LOC_LOCF', 'LOC_RT',
		'TRACE_ID',
	) . "\n";

	for my $results_item (@{$results}) {
		print $file join(' ', @{$results_item}) . "\n";
	}

	close($file);
	return;
}

sub catch_signal {
	my $signame = shift;
	print STDERR "Received SIG$signame signal\n";
	$_->kill('INT')->detach() for @threads;
	return;
}


