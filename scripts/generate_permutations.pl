#!/usr/bin/env perl
use strict;
use warnings;

use Algorithm::Permute;
use Algorithm::Combinatorics qw(combinations);
use Data::Dumper;
use Log::Log4perl qw(get_logger :no_extra_logdie_message);

use Platform;

Log::Log4perl::init('log4perl.conf');

my $logger = get_logger('test');

my @levels = (1, 2, 4, 8);
my @available_cpus = (0..($levels[$#levels] - 1));
my $removed_cpus_number = 24;
my $required_cpus = 8;
my $permutations_file_name = "/tmp/permutations";
my $execution_id = 1;

for my $i (0..($removed_cpus_number - 1)) {
	my $position = int(rand($levels[$#levels] - 1 - $i));
	splice(@available_cpus, $position, 1);
}

# Put everything in the log file
$logger->info("platform: @levels");
$logger->info("available cpus: @available_cpus");
$logger->info("removed cpus: $removed_cpus_number");
$logger->info("required cpus: $required_cpus");

my $platform = Platform->new(\@levels, \@available_cpus, 1);
$platform->build_structure();
$platform->build_platform_xml();
$platform->save_platform_xml('platform.xml');

open(my $file, '>', $permutations_file_name);

my $iterator = Algorithm::Permute->new(\@available_cpus);
while (my @permutation = $iterator->next()) {
	print $file join('-', @permutation) . "\n";
}

sub get_log_file {
	return "generate_permutations.log";
}


