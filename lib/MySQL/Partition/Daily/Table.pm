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

sub partition_name_format {
    my $self = shift;
    $self->{partition_name_format} ||= 'p%Y%m%d';
}

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
    my $pname_format = $self->partition_name_format;

    my $now = Time::Piece->localtime;
    my $tomorrow = $now + ONE_DAY;

    my $today_name     = $now->strftime($pname_format);
    my $tomorrow_name  = $tomorrow->strftime($pname_format);

    if (!$mysql_partition->is_partitioned) {

        $mysql_partition->create_partitions(
            $today_name     => $self->_partition_name2description($today_name),
            $tomorrow_name  => $self->_partition_name2description($tomorrow_name),
        );
    }
    else {
        if ($mysql_partition->has_partition($tomorrow_name) ) {
            warn "@{[$self->table]} already has $tomorrow_name partition\n";
            return;
        }

        if ($self->catch_all_partition_name) {
            $mysql_partition->reorganize_catch_all_partition(
                $tomorrow_name  => $self->_partition_name2description($tomorrow_name),
            );
        }
        else {
            $mysql_partition->add_partitions(
                $tomorrow_name  => $self->_partition_name2description($tomorrow_name),
            );
        }

        if (my $days = $self->keep_days) {
            my @drop_partition_names;
            my $drop_date = $now - (ONE_DAY * $days);
            while (1) {
                my $drop_target_name = $drop_date->strftime($pname_format);
                if ($mysql_partition->has_partition($drop_target_name)) {
                    push @drop_partition_names, $drop_target_name;
                    $drop_date = $drop_date - ONE_DAY;
                }
                else {
                    last;
                }
            }

            if (@drop_partition_names) {
                $mysql_partition->drop_partitions(@drop_partition_names);
            }
        }
    }
}

sub _partition_name2description {
    my ($self, $partition_name) = @_;

    (Time::Piece->strptime($partition_name, $self->partition_name_format) + ONE_DAY)->strftime('%Y-%m-%d');
}

1;
