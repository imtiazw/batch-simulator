package Basic;
use strict;
use warnings;

use lib 'ProcessorRange/blib/lib', 'ProcessorRange/blib/arch';
use ProcessorRange;

use Data::Dumper;

sub new {
	my $class = shift;

	my $self = {};

	bless $self, $class;
	return $self;
}

sub reduce {
	my $self = shift;
	my $target_number = shift;
	my $left_processors = shift;

	my @remaining_ranges;

	$left_processors->ranges_loop(
		sub {
			my ($start, $end) = @_;
			my $taking = $target_number;
			my $available_processors = $end + 1 - $start;

			$taking = $available_processors if ($available_processors < $target_number);

			push @remaining_ranges, [$start, $start + $taking - 1];
			$target_number -= $taking;
			return ($target_number != 0);
		}
	);

	$left_processors->affect_ranges(ProcessorRange::sort_and_fuse_contiguous_ranges(\@remaining_ranges));
	return;
}

1;

