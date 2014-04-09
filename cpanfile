requires 'Class::Accessor::Lite';
requires 'MySQL::Partition';
requires 'Time::Piece';
requires 'Time::Seconds';
requires 'perl', '5.008001';

on configure => sub {
    requires 'CPAN::Meta';
    requires 'CPAN::Meta::Prereqs';
    requires 'Module::Build';
};

on test => sub {
    requires 'Test::More';
};
