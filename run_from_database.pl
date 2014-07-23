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
use Database;

my ($trace_number, $cpus_number) = @ARGV;
die 'missing arguments: trace_number cpus_number' unless defined $cpus_number;

my $database = Database->new();
my $trace = Trace->new_from_database($trace_number);
$trace->write_to_file("run_from_database-$trace_number-$cpus_number.swf");

#my $schedule_fcfs = FCFS->new($trace, $cpus_number);
#$schedule_fcfs->run();
#print "FCFS: " . $schedule_fcfs->cmax() . "\n";
#$schedule_fcfs->save_svg("backfilling_FCFS-$trace_number-$cpus_number-fcfs.svg");

#my $schedule_fcfsc= FCFSC->new($trace, $cpus_number);
#$schedule_fcfsc->run();
#print "FCFSC " . $schedule_fcfsc->cmax() . "\n";
#$schedule_fcfsc->save_svg("backfilling_FCFS-$trace_number-$cpus_number-fcfsc.svg");

my $schedule_backfilling= Backfilling->new($trace, $cpus_number);
$schedule_backfilling->run();
print "Backfilling " . $schedule_backfilling->cmax() . "\n";
$schedule_backfilling->save_svg("run_from_database-$trace_number-$cpus_number-backfilling.svg");

my $schedule_backfilling_contiguous= Backfilling->new($trace, $cpus_number, 1);
$schedule_backfilling_contiguous->run();
print "Backfilling contiguous " . $schedule_backfilling_contiguous->cmax() . "\n";
$schedule_backfilling_contiguous->save_svg("run_from_database-$trace_number-$cpus_number-backfilling_contiguous.svg");

