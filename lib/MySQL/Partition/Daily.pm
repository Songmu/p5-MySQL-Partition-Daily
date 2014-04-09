package MySQL::Partition::Daily;
use 5.008001;
use strict;
use warnings;

our $VERSION = "0.01";

use MySQL::Partition::Daily::Table;

use Class::Accessor::Lite (
    ro => [qw/tables dbh catch_all_partition_name/],
);

sub partition_tables {
    my $self = shift;

    $self->{partition_tables} ||= do {
        [
            map {
                my $table = $_;
                my %args  = %{ $self->tables->{$table} };
                if (!$args{catch_all_partition_name} && $self->catch_all_partition_name) {
                    $args{catch_all_partition_name} = $self->catch_all_partition_name;
                }
                MySQL::Partition::Daily::Table->new(
                    dbh   => $self->dbh,
                    table => $table,
                    %args,
                );
            } keys %{ $self->tables }
        ]
    };
}

sub run {
    my $self = shift;
    for my $partition_table (@{ $self->partition_tables }) {
        $partition_table->rotate_partition;
    }
}

1;
__END__

=encoding utf-8

=head1 NAME

MySQL::Partition::Daily - It's new $module

=head1 SYNOPSIS

    use MySQL::Partition::Daily;

=head1 DESCRIPTION

MySQL::Partition::Daily is ...

=head1 LICENSE

Copyright (C) Songmu.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=head1 AUTHOR

Songmu E<lt>y.songmu@gmail.comE<gt>

=cut

