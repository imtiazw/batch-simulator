package Database;
use strict;
use warnings;

use DBI;
use Data::Dumper qw(Dumper);

use Trace;
use Job;

sub new {
	my $class = shift;
	my $self = {
		driver => "mysql",
		database => "parser",
		userid => "parser",
		password => "parser"
	};

	$self->{dsn} = "DBI:$self->{driver}:database=$self->{database}";
	$self->{dbh} = DBI->connect($self->{dsn}, $self->{userid}, $self->{password}) or die $DBI::errstr;
	
	bless $self, $class;
	return $self;
}

sub prepare_tables {
	my $self = shift;

	$self->{dbh}->do("CREATE TABLE IF NOT EXISTS executions (
		id INT NOT NULL AUTO_INCREMENT,
		trace_file VARCHAR(255),
		jobs_number INT,
		executions_number INT,
		cpus_number INT,
		threads_number INT,
		git_revision VARCHAR(255),
		run_time INT,
		PRIMARY KEY (id)
	)");

	$self->{dbh}->do("CREATE TABLE IF NOT EXISTS traces (
		id INT NOT NULL AUTO_INCREMENT,
		execution INT NOT NULL,
		PRIMARY KEY(id),
		FOREIGN KEY (execution) REFERENCES executions(id)
	)");

	$self->{dbh}->do("CREATE TABLE IF NOT EXISTS jobs (
		id INT NOT NULL AUTO_INCREMENT,
		trace INT NOT NULL,
		job_number INT,
		submit_time INT,
		wait_time INT,
		run_time INT,
		allocated_cpus INT,
		avg_cpu_time INT,
		used_mem INT,
		requested_cpus INT,
		requested_time INT,
		requested_mem INT,
		status INT,
		uid INT,
		gid INT,
		exec_number INT,
		queue_number INT,
		partition_number INT,
		prec_job_number INT,
		think_time_prec_job INT,
		assigned_processors VARCHAR(255),
		starting_time INT,
		PRIMARY KEY (id),
		FOREIGN KEY (trace) REFERENCES traces(id)
	)");
}

sub add_execution {
	my $self = shift;
	my $execution = shift;

	my $key_string = join (',', keys %{$execution});
	my $value_string = join ('\',\'', values %{$execution});
	$self->{dbh}->do("INSERT INTO executions ($key_string) values (\'$value_string\')");

	my $sth = $self->{dbh}->prepare("SELECT MAX(id) AS id FROM executions");
	$sth->execute();
	my $ref = $sth->fetchrow_hashref();
	return $ref->{id};
}

sub update_execution_run_time {
	my $self = shift;
	my $execution_id = shift;
	my $run_time = shift;

	$self->{dbh}->do("UPDATE executions SET run_time = \'$run_time\' WHERE id = \'$execution_id\'");
}

sub add_trace {
	my $self = shift;
	my $trace = shift;
	my $execution_id = shift;

	$self->{dbh}->do("INSERT INTO traces (execution) VALUES (\'$execution_id\')");

	my $sth = $self->{dbh}->prepare("SELECT MAX(id) AS id FROM traces");
	$sth->execute();
	my $ref = $sth->fetchrow_hashref();
	my $trace_id = $ref->{id};

	# Add the jobs
	for my $job (@{$trace->jobs()}) {
		my $key_string = join (',', keys %{$job});
		my $value_string = join ('\',\'', values %{$job});
		$self->{dbh}->do("INSERT INTO jobs (trace, $key_string) values (\'$trace_id\', \'$value_string\')");
	}

	return $trace_id;
}

sub get_trace_ref {
	my $self = shift;
	my $trace_id = shift;

	my $sth = $self->{dbh}->prepare("SELECT * FROM traces WHERE id=\'$trace_id\'");
	$sth->execute();
	return $sth->fetchrow_hashref();
}

sub get_job_refs {
	my $self = shift;
	my $trace_id = shift;
	my $sth = $self->{dbh}->prepare("SELECT * FROM jobs WHERE trace=\'$trace_id\'");
	$sth->execute();

	my @job_refs;
	while (my $job_ref = $sth->fetchrow_hashref()) {
		push @job_refs, $job_ref;
	}
	return @job_refs;
}

1;