package Platform;
use strict;
use warnings;

use Log::Log4perl qw(get_logger);
use Data::Dumper;
use List::Util qw(min max sum);
use POSIX;
use XML::Smart;
use Carp;

use Tree;

# Default power, latency and bandwidth values
use constant CLUSTER_POWER => "23.492E9";
use constant CLUSTER_BANDWIDTH => "1.25E9";
use constant CLUSTER_LATENCY => "1.0E-4";
use constant LINK_BANDWIDTH => "1.25E9";
use constant LINK_LATENCY => "5.0E-2";

# Constructors

sub new {
	my $class = shift;
	my $levels = shift;

	my $self = {
		levels => $levels,
	};

	bless $self, $class;
	return $self;
}

# Platform structure generation code.
# This is the exact version of the algorithm. It builds a list of all the
# possible combination of CPUs and checks to see which one is the best. Takes a
# long time in normal sized platforms.
sub build {
	my $self = shift;
	my $available_cpus = shift;

	$self->{root} = $self->_build(0, 0, $available_cpus);
	return;
}

sub build_structure {
	my $self = shift;
	my $available_cpus = shift;

	my $last_level = $#{$self->{levels}} - 1;
	my $cluster_size = $self->{levels}->[$#{$self->{levels}}]/$self->{levels}->[$#{$self->{levels}} - 1];

	my @cpus_structure;

	for my $level (0..$last_level) {
		$cpus_structure[$level] = [];

		my $nodes_per_block = $self->{levels}->[$last_level]/$self->{levels}->[$last_level - $level];

		for my $block (0..($self->{levels}->[$last_level - $level] - 1)) {
			my $block_content = {
				total_size => 0,
				total_original_size => $self->{levels}->[$#{$self->{levels}}]/$self->{levels}->[$last_level - $level],
				cpus => []
			};

			for my $cluster (($block * $nodes_per_block)..(($block + 1) * $nodes_per_block - 1)) {
				next unless (defined $available_cpus->[$cluster]);

				$block_content->{total_size} += $available_cpus->[$cluster]->{total_size};
				push @{$block_content->{cpus}}, @{$available_cpus->[$cluster]->{cpus}};
			}

			push @{$cpus_structure[$level]}, $block_content;
		}
	}

	return \@cpus_structure;
}

sub choose_combination {
	my $self = shift;
	my $requested_cpus = shift;

	$self->_score($self->{root}, 0, $requested_cpus);
	return $self->_choose_combination($self->{root}, 0, $requested_cpus);
}

sub _choose_combination {
	my $self = shift;
	my $tree = shift;
	my $level = shift;
	my $requested_cpus = shift;

	# Return nothing if requested_cpus is 0
	return unless ($requested_cpus);

	# Return if at the last level
	return [$tree->content()->{id}, $requested_cpus] if ($level == $#{$self->{levels}} - 1);

	my $best_combination = $tree->content()->{$requested_cpus}->{combination};

	my @children = @{$tree->children()};
	return map {$self->_choose_combination($_, $level + 1, shift @{$best_combination})} (@children);
}

sub choose_cpus {
	my $self = shift;
	my $requested_cpus = shift;

	$self->_score($self->{root}, 0, $requested_cpus);
	return $self->_choose_cpus($self->{root}, $requested_cpus);
}

sub _choose_cpus {
	my $self = shift;
	my $tree = shift;
	my $requested_cpus = shift;

	# No requested cpus
	return unless $requested_cpus;

	my @children = @{$tree->children()};

	# Leaf node/CPU
	return $tree->content()->{id} if (defined $tree->content()->{id});

	my $best_combination = $tree->content()->{$requested_cpus};

	my @combination_parts = split('-', $best_combination->{combination});

	return map {$self->_choose_cpus($_, shift @combination_parts)} (@children);
}

sub _build {
	my $self = shift;
	my $level = shift;
	my $node = shift;
	my $available_cpus = shift;

	my $next_level_nodes = $self->{levels}->[$level + 1]/$self->{levels}->[$level];
	my @next_level_nodes_ids = map {$next_level_nodes * $node + $_} (0..($next_level_nodes - 1));

	# Last level before the leafs/nodes
	if ($level == $#{$self->{levels}} - 1) {
		my $tree_content = {
			total_size => (defined $available_cpus->[$node]) ? $available_cpus->[$node] : 0,
			nodes => [@next_level_nodes_ids],
			id => $node
		};
		return Tree->new($tree_content);
	}

	my @children = map {$self->_build($level + 1, $_, $available_cpus)} (@next_level_nodes_ids);

	my $total_size = 0;
	$total_size += $_->content()->{total_size} for (@children);

	my $tree_content = {total_size => $total_size, id => $node};
	my $tree = Tree->new($tree_content);
	$tree->children(\@children);
	return $tree;
}

sub _combinations {
	my $self = shift;
	my $tree = shift;
	my $requested_cpus = shift;
	my $node = shift;

	my @children = @{$tree->children()};
	my $last_child = $#children;

	# Last node
	return $requested_cpus if ($node == $last_child);

	my @remaining_children = @children[($node + 1)..$last_child];
	my $remaining_size = sum (map {$_->content()->{total_size}} (@remaining_children));

	my $minimum_cpus = max(0, $requested_cpus - $remaining_size);
	my $maximum_cpus = min($children[$node]->content()->{total_size}, $requested_cpus);

	my @combinations;

	for my $cpus_number ($minimum_cpus..$maximum_cpus) {
		my @children_combinations = $self->_combinations($tree, $requested_cpus - $cpus_number, $node + 1);
		push @combinations, [$cpus_number, $_] for (@children_combinations);
	}

	return @combinations;
}

sub _score {
	my $self = shift;
	my $tree = shift;
	my $level = shift;
	my $requested_cpus = shift;

	# No needed CPUs
	return 0 unless $requested_cpus;

	my $max_depth = $#{$self->{levels}} - 1;

	# Leaf/CPU
	return 0 if ($level == $max_depth);

	# Best combination already saved
	return $tree->content()->{$requested_cpus}->{score} if (defined $tree->content()->{$requested_cpus});

	my @children = @{$tree->children()};
	my $last_child = $#children;
	my @combinations = $self->_combinations($tree, $requested_cpus, 0);
	my %best_combination = (score => LONG_MAX, combination => '');

	for my $combination (@combinations) {
		my $score = 0;

		for my $child_id (0..$last_child) {
			my $child_size = $children[$child_id]->content()->{total_size};
			my $child_requested_cpus = $combination->[$child_id];

			my $child_score = $self->_score($children[$child_id], $level + 1, $child_requested_cpus);
			$score = max($score, $child_score);
		}

		# Add to the score if there is communication between different child nodes
		$score += ($max_depth + 1 - $level) if (max(@{$combination}) < $requested_cpus);

		if ($score < $best_combination{score}) {
			$best_combination{score} = $score;
			$best_combination{combination} = $combination;
		}
	}

	$tree->content()->{$requested_cpus} = \%best_combination;
	return $best_combination{score};
}

# Platform XML generation code
# This code will be used to generate platform files and host files to be used
# with SMPI initially.
sub build_platform_xml {
	my $self = shift;

	my @platform_parts = @{$self->{levels}};
	my $cluster_size = $platform_parts[$#platform_parts]/$platform_parts[$#platform_parts - 1];
	my $xml = XML::Smart->new();

	$xml->{platform} = {version => 3};

	# Root system
	$xml->{platform}{AS} = {
		id => "AS_Root",
		routing => "Floyd",
	};

	# Tree system
	$xml->{platform}{AS}{AS} = {
		id => "AS_Tree",
		routing => "Floyd",
	};

	# Push the first router
	push @{$xml->{platform}{AS}{AS}{router}}, {id => "R-0-0"};

	# Build levels
	for my $level (1..($#platform_parts - 1)) {
		my $nodes_number = $platform_parts[$level];

		for my $node_number (0..($nodes_number - 1)) {
			push @{$xml->{platform}{AS}{AS}{router}}, {id => "R-$level-$node_number"};

			my $father_node = int $node_number/($platform_parts[$level]/$platform_parts[$level - 1]);
			push @{$xml->{platform}{AS}{AS}{link}}, {
				id => "L-$level-$node_number",
				bandwidth => LINK_BANDWIDTH,
				latency => LINK_LATENCY,
			};

			push @{$xml->{platform}{AS}{AS}{route}}, {
				src => 'R-' . ($level - 1) . "-$father_node",
				dst => "R-$level-$node_number",
				link_ctn => {id => "L-$level-$node_number"},
			};
		}
	}

	# Master host
	push @{$xml->{platform}{AS}{cluster}}, {
			id => 'C-MH',
			prefix => 'master_host',
			suffix => '',
			radical => '0-0',
			power => CLUSTER_POWER,
			bw => CLUSTER_BANDWIDTH,
			lat => CLUSTER_LATENCY,
			router_id => 'R-MH',
	};

	push @{$xml->{platform}{AS}{link}}, {
		id => 'L-MH',
		bandwidth => LINK_BANDWIDTH,
		latency => LINK_LATENCY,
	};

	push @{$xml->{platform}{AS}{ASroute}}, {
		src => 'C-MH',
		gw_src => 'R-MH',
		dst => 'AS_Tree',
		gw_dst => 'R-0-0',
		link_ctn => {id => 'L-MH'},
	};

	# Clusters
	for my $cluster (0..($platform_parts[$#platform_parts - 1] - 1)) {
		push @{$xml->{platform}{AS}{cluster}}, {
			id => "C-$cluster",
			prefix => "",
			suffix => "",
			radical => ($cluster * $cluster_size) . '-' . (($cluster + 1) * $cluster_size - 1),
			power => CLUSTER_POWER,
			bw => CLUSTER_BANDWIDTH,
			lat => CLUSTER_LATENCY,
			router_id => "R-$cluster",
		};

		push @{$xml->{platform}{AS}{link}}, {
			id => "L-$cluster",
			bandwidth => LINK_BANDWIDTH,
			latency => LINK_LATENCY,
		};

		push @{$xml->{platform}{AS}{ASroute}}, {
			src => "C-$cluster",
			gw_src => "R-$cluster",
			dst => "AS_Tree",
			gw_dst => 'R-' . ($#platform_parts - 1) . "-$cluster",
			link_ctn => {id => "L-$cluster"},
		}
	}

	$self->{xml} = $xml;
	return;
}

sub save_platform_xml {
	my $self = shift;
	my $filename = shift;

	open(my $file, '>', $filename);

	print $file "<?xml version=\'1.0\'?>\n" . "<!DOCTYPE platform SYSTEM \"http://simgrid.gforge.inria.fr/simgrid.dtd\">\n" . $self->{xml}->data(noheader => 1, nometagen => 1);

	return;
}

sub save_hostfile {
	my $cpus = shift;
	my $filename = shift;

	open(my $file, '>', $filename);
	print $file join("\n", @{$cpus}) . "\n";

	return;
}

sub generate_all_combinations {
	my $self = shift;
	my $requested_cpus = shift;

	return $self->_combinations($self->{root}, $requested_cpus, 0);
}

sub _score_function_pnorm {
	my $self = shift;
	my $child_requested_cpus = shift;
	my $requested_cpus = shift;
	my $level = shift;

	my $max_depth = scalar @{$self->{levels}} - 1;

	return $child_requested_cpus * ($requested_cpus - $child_requested_cpus) * pow(($max_depth - $level) * 2, $self->{norm});
}

sub level_distance {
	my $self = shift;
	my $first_node = shift;
	my $second_node = shift;

	my $last_level = $#{$self->{levels}};

	for my $level (0..($last_level - 1)) {
		my $cpus_group = $self->{levels}->[$last_level]/$self->{levels}->[$level + 1];
		return $last_level - $level if (int $first_node/$cpus_group != int $second_node/$cpus_group);
	}

	return 0;
}

sub generate_speedup {
	my $self = shift;
	my $benchmark = shift;

	my $smpi_script = './scripts/smpi/smpireplay.sh';
	my $platform_file = '/tmp/platform';
	my $hosts_file = '/tmp/hosts';

	$self->build_platform_xml();
	$self->save_platform_xml($platform_file);

	my $last_level = $#{$self->{levels}};
	my @hosts_configs = map {[0, $self->{levels}->[$_]]} (0..($last_level - 1));
	my $cpus_number = $self->{levels}->[$last_level]/$self->{levels}->[$last_level - 1];

	my @results;

	for my $hosts_config (@hosts_configs) {
		save_hosts_file($hosts_config, $hosts_file);

		my $result = `$smpi_script $cpus_number $platform_file $hosts_file $benchmark 2>&1`;

		unless ($result =~ /Simulation time (\d*\.\d*)/) {
			print STDERR "$smpi_script $cpus_number $platform_file $hosts_file $benchmark\n";
			print STDERR "$result\n";
			die 'error running benchmark';
		}

		my $distance = $self->level_distance($hosts_config->[0], $hosts_config->[1]);
		push @results, $1;
	}

	my $base_runtime = $results[0];
	@results = map {$_/$base_runtime} (@results);

	return @results;
}

sub save_hosts_file {
	my $hosts_config = shift;
	my $hosts_file = shift;

	open(my $file, '>', $hosts_file);
	print $file join("\n", @{$hosts_config}) . "\n";
	close($file);
}

1;
