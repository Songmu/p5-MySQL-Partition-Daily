use strict;
use warnings;
use utf8;
use Test::MockTime qw/set_relative_time/;
use Test::More 0.98;
use Test::mysqld;

use Time::Seconds;

my $mysqld = Test::mysqld->new(
    my_cnf => {
      'skip-networking' => '',
    }
) or plan skip_all => $Test::mysqld::errstr;

my @connect_info = ($mysqld->dsn(dbname => 'test'));
$connect_info[3] = {
    RaiseError          => 1,
    PrintError          => 0,
    ShowErrorStatement  => 1,
    AutoInactiveDestroy => 1,
    mysql_enable_utf8   => 1,
};
my $dbh = DBI->connect(@connect_info);

use MySQL::Partition::Daily;

$dbh->do(q[CREATE TABLE `test1` (
  `id` BIGINT unsigned NOT NULL auto_increment,
  `created_at` datetime NOT NULL,
  PRIMARY KEY (`id`, `created_at`)
)]);

$dbh->do(q[CREATE TABLE `test2` (
  `id` BIGINT unsigned NOT NULL auto_increment,
  `last_accessed_at` datetime NOT NULL,
  PRIMARY KEY (`id`, `last_accessed_at`)
)]);

sub partition_daily {
    MySQL::Partition::Daily->new(
        dbh    => $dbh,
        catch_all_partition_name => 'pmax',
        tables => {
            test1 => {
                column => 'created_at',
            },
            test2 => {
                column    => 'last_accessed_at',
                keep_days => 1,
            },
        },
    );
}

partition_daily->run;
pass 'create ok';

set_relative_time(ONE_DAY);
partition_daily->run;
pass 'add ok';

partition_daily->run;
pass 'nop ok';

set_relative_time(ONE_DAY * 2);
partition_daily->run;
pass 'add and drop ok';

done_testing;
