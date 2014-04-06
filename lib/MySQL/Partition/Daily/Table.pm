package MySQL::Partition::Daily::Table;
use strict;
use warnings;

use MySQL::Partition;
use Time::Moment;

use Mouse;

has [qw/table column/] => (is => 'ro', isa => 'Str',    required => 1);
has dbh                => (is => 'ro', isa => 'Object', required => 1);
has keep_days          => (is => 'ro', isa => 'Int',    default  => 0); # 0 as no limit
has catch_all_partition_name => (is => 'ro', isa => 'Str');

has mysql_partition => (
    is      => 'ro',
    isa     => 'MySQL::Partition::Type::Range',
    lazy    => 1,
    default => sub {
        my $self = shift;
        MySQL::Partition->new(
            type       => 'range columns',
            dbh        => $self->dbh,
            table      => $self->table,
            expression => $self->column,
        );
    },
);

no Mouse;

sub rotate_partition {
    my $self = shift;
    my $mysql_partition = $self->mysql_partition;

    if (!$mysql_partition->is_partitioned) {
        # datetime
        my $starting_point      = Time::Moment->now;
        my $next_point          = $starting_point->plus_days(1);
        my $before_point        = $starting_point->minus_days(1);

        my $before_point_date   = $before_point->strftime('%Y-%m-%d');

        my $starting_point_name = $before_point->strftime('p%Y%m%d');
        my $starting_point_date = $starting_point->strftime('%Y-%m-%d');

        my $next_point_name     = $starting_point->strftime('p%Y%m%d');
        my $next_point_date     = $next_point->strftime('%Y-%m-%d');

        my $tomorrow_name       = $next_point->strftime('p%Y%m%d');
        my $tomorrow_point_date = $next_point->plus_days(1)->strftime('%Y-%m-%d');

        $mysql_partition->create_partitions(
            $starting_point_name => $starting_point_date,
            $next_point_name     => $next_point_date,
            $tomorrow_name       => $tomorrow_point_date,
        );
    }
    else {
        my $today    = Time::Moment->now;
        my $tomorrow = $today->plus_days(1);
        my $day_after_tomorrow = $tomorrow->plus_days(1);

        my $next_point_date = $day_after_tomorrow->strftime('%Y-%m-%d');
        my $next_point_name = $tomorrow->strftime('p%Y%m%d');

        if ($mysql_partition->has_partition($next_point_name) ) {
            warn "@{[$self->table]} already has $next_point_name partition\n";
            return;
        }

        if ($self->catch_all_partition_name) {
            $mysql_partition->reorganize_catch_all_partition(
                $next_point_name => $next_point_date,
            );
        }
        else {
            $mysql_partition->add_partitions(
                $next_point_name => $next_point_date,
            );
        }
    }
}

1;
