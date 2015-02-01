#!/usr/bin/env perl
use strict;
use warnings;

use Data::Dumper qw(Dumper);

use Trace;
use Backfilling;

my ($trace_file, $jobs_number, $cpus_number) = @ARGV;
my $cluster_size = 16;

my $trace = Trace->new_from_swf($trace_file);
$trace->remove_large_jobs($cpus_number);
$trace->keep_first_jobs($jobs_number);
$trace->fix_submit_times();
$trace->write_to_file("experiment_fred3/$jobs_number-$cpus_number.swf");
my $schedule = Backfilling->new(REUSE_EXECUTION_PROFILE, $trace, $cpus_number, $cluster_size, BASIC);
$schedule->run();

print "$jobs_number $cpus_number " . $schedule->{schedule_time} . "\n";

