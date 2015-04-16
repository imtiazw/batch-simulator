#!/usr/bin/env perl
use strict;
use warnings;

use Data::Dumper qw(Dumper);
use Log::Log4perl qw(get_logger);

use Trace;
use Backfilling;

my ($trace_file, $cpus_number, $cluster_size, $variant) = @ARGV;

Log::Log4perl::init('log4perl.conf');
my $logger = get_logger();

my $trace = Trace->new_from_swf($trace_file);
my $schedule = Backfilling->new($trace, $cpus_number, $cluster_size, $variant);
$schedule->run();

my @results = (
	$schedule->cmax(),
	$schedule->contiguous_jobs_number(),
	$schedule->local_jobs_number(),
	$schedule->locality_factor(),
	$schedule->run_time(),
);

print STDOUT join(' ', @results);

