package MySQL::Partition::Daily::Table;
use strict;
use warnings;

use MySQL::Partition;
use Time::Piece ();
use Time::Seconds;

use Class::Accessor::Lite (
    new => 1,
    ro => [qw/table column dbh keep_days catch_all_partition_name/],
);

sub mysql_partition {
    my $self = shift;
    $self->{mysql_partition} ||= MySQL::Partition->new(
        type       => 'range columns',
        dbh        => $self->dbh,
        table      => $self->table,
        expression => $self->column,
    );
}

sub rotate_partition {
    my $self = shift;
    my $mysql_partition = $self->mysql_partition;

    my $now = Time::Piece->localtime;
    my $tomorrow = $now + ONE_DAY;

    my $today_name     = $now->strftime('p%Y%m%d');
    my $tomorrow_name  = $tomorrow->strftime('p%Y%m%d');

    if (!$mysql_partition->is_partitioned) {

        $mysql_partition->create_partitions(
            $today_name     => _partition_name2description($today_name),
            $tomorrow_name  => _partition_name2description($tomorrow_name),
        );
    }
    else {
        if ($mysql_partition->has_partition($tomorrow_name) ) {
            warn "@{[$self->table]} already has $tomorrow_name partition\n";
            return;
        }

        if ($self->catch_all_partition_name) {
            $mysql_partition->reorganize_catch_all_partition(
                $tomorrow_name  => _partition_name2description($tomorrow_name),
            );
        }
        else {
            $mysql_partition->add_partitions(
                $tomorrow_name  => _partition_name2description($tomorrow_name),
            );
        }
    }
}

sub _partition_name2description {
    my $partition_name = shift;

    (Time::Piece->strptime($partition_name, 'p%Y%m%d') + ONE_DAY)->strftime('%Y-%m-%d');
}

1;
