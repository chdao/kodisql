#!/usr/bin/perl
use DBI;
use experimental 'smartmatch';
use Getopt::Long;


$result = GetOptions    ("user=s" => \$USER,
             "pass=s" => \$PASS,
             "target=s" => \$TARGET,
             "host=s" => \$HOST,
             "h=s" => \$HOST,
             "u=s" => \$USER,
             "p=s" => \$PASS,
             "port=i" => \$PORT,
             "data=s" => \$DATA,
             "main=s" => \$MAINDB,
             "help" => \$HELP);


if ((!($USER)) || (!($PASS)) || (!($TARGET)) || (!($MAINDB))) {
   &help;
   exit;
}

if (!($PORT)) {
  $PORT = "3306";
}

if (!($DATA)) {
  $DATA="kodisql";
}

$mysqlhost="$HOST";
$mysqluser="$USER";
$mysqlpass="$PASS";
$mysqlport="$PORT";


# Temporary settings.. will be command line options later
$target = "$TARGET";
# The master kodi database to replicate from (whatever is declared in your advancedsettings.xml).
$maindb = "$MAINDB";


$globaltable = "files";
@file_exceptions = ("playCount","lastPlayed");

# The database where we keep the list of clients and other info we may need on upgrades.
$datadb = "$DATA";

################################################
# Connect to the database.
sub connectdb {
  $dbh = DBI->connect("DBI:mysql:host=$mysqlhost", "$mysqluser", "$mysqlpass", {'RaiseError' => 1});
  @dbs = $dbh->func('_ListDBs');
  foreach (@dbs) {
    if ($_ =~ /${maindb}\d+/) {
      push @kodidbs, $_;
    }
  }
  my $highdb;
  my $highrev;
  foreach (@kodidbs) {
    $db = $_;
    $rev = $_;;
    $highrev = $highdb;
    ($rev) = $rev =~ m/${maindb}(\d+)/;
    ($highrev) = $highdb =~ m/${maindb}(\d+)/;
    if (($rev > $highrev) || (!$highrev)) {
      $highrev = $rev;
      $highdb = "${maindb}$highrev";
    }
    if ((($oldrev < $ref) && ($rev < $highref)) || (!($oldrev))) {
      my $exists = &checkifexists("$target$oldrev");
      if ($exists) {
        $oldrev = $rev;
      }
    }
  }
  return("$highrev","$highdb");
}

sub help {
  print "MySQL-Kodi Dynamic database replicator\n";
  print "wickedsun 2016\n";
  print "  --user,-u\t\tSet MySQL USERNAME\t(required)\n";
  print "  --pass,-p\t\tSet MySQL PASSWORD\t(required)\n";
  print "  --port\t\tSet MySQL PORT\t\t(Default: 3306)\n";
  print "  --target\t\tSet TARGET Database\t(required)\n";
  print "  --main\t\tSet MAIN Database\t(required)\n";
  print "  --config\t\tSet CONFIG Database\t(Default: kodisql_config)\n";
  print "  --updateall\t\tUpdate all slave DBs to current schema (not yet implemented!)\n";
  print "  --help\t\tThis help message\n";
}

# check if the target db exists and create it if it doesn't
sub checkdb {
  my $db = @_[0];
  my $exists = &checkifexists($db);
  if (!($exists)) {
    $dbh->do("CREATE DATABASE $db");
  }
}


sub main {
  my ($highrev,$highdb) = &connectdb;
  print "HIGH: $highdb\n";
  &checkdb("$DATA");
  &checkdb("$target$highrev");
  &checkslavecol("$target","$highrev");
  &createviews("$highdb","$target","$highrev");
  my %view;
  &createviewhash("$highdb","$target","$highrev");
  &recreatehighviews("$target","$highrev");
}

sub checkifexists {
  my ($db,$table,$column) = @_;
  if ($column) {
    $sth = $dbh->prepare("SELECT * FROM information_schema.columns WHERE table_schema = '$db' AND table_name = '$table' AND COLUMN_NAME = '$column'");
  } elsif ($table) {
      $sth = $dbh->prepare("SELECT * FROM information_schema.columns WHERE table_schema = '$db' AND table_name = '$table'");
  } else {
    $sth = $dbh->prepare("SELECT * FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = '$db'");
  }
  $sth->execute;
  my $rows = $sth->rows;
  $sth->finish;
  if ($rows > 0) {
    print "table/column exists -- $db $table $column \n";
    return 1;
  } else {
    print "table/column does not exists -- $db $table $column\n";
    return;
  }
}

sub createviews {
  ($db,$target,$rev) = @_;
  $sth = $dbh->prepare("SHOW FULL TABLES IN $db WHERE TABLE_TYPE LIKE 'VIEW'");
  $sth->execute;
  my @views;
  while (my $ref = $sth->fetchrow_hashref()) {
    my $view = $ref->{"Tables_in_$db"};
    push @views, $view;
  }
  $dbh->do("USE $db");
  my $exists = &checkifexists($db,"files","${target}_playCount");
  if (!($exists)) {
    $dbh->do("ALTER TABLE `$db`.`files` ADD `${target}_playCount` INT( 11 ) NULL DEFAULT NULL , ADD `${target}_lastPlayed` TEXT CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL");
  }
  print "$db\n";
  $dbh->do("USE $db");
  $sth = $dbh->prepare("SHOW COLUMNS FROM $globaltable");
  $sth->execute;
  my $statement;
  my $header;
  my $tail;
  my $exists;
  while (my $ref = $sth->fetchrow_hashref()) {
    my $field = $ref->{'Field'};
    foreach (@file_exceptions) {
      $exc = $_;
      if ($field eq $exc) {
    # CREATE VIEW `User1Videos93`.`files` AS idFile, idPath, strFilename, playCount1 AS playCount, lastPlayed1 AS lastPlayed, dateAdded FROM `MyVideos93`.`globalfiles`;
        push @statement, "files.${target}_$exc AS $exc";
        #push @statement_main, "${maindb}_$exc AS $exc";
        print "Pushed ${maindb}_$exc AS $exc\n";
      }
    }
    if ((!($field ~~ @statement)) && (!($field ~~ @file_exceptions))) {
      my $skip = 0;
      foreach (@file_exceptions) {
        my $except = $_;
        if ($field =~ m/.*_$except/) { $skip = 1;}
      }
      if ($skip == 0) {
        push @statement, "files.${field}";
        push @statement_main, "$field";
      }
    }
  }
  foreach (@statement) {
      my $part = $_;
      if ($statement) {
    $statement = "$statement, $part";
    $statement_main = "$statement_main, $part";
      } else {
    $statement = "$part";
    $statement_main = "$part";
      }
  }
  $header = "CREATE VIEW `${target}${rev}`.`files` AS SELECT";
  $header_main = "CREATE VIEW `$db`.`files` AS SELECT";
  $footer = "FROM `$db`.`$globaltable`";
  $footer_main = "FROM `$db`.`$globaltable`";
  $exists = &checkifexists("${target}${rev}","files");
  if (!($exists)) {
    print "creating VIEW: $header $statement $footer\n";
    $dbh->do("$header $statement $footer");
  }
  $exists = &checkifexists("$db","files");
  if (!($exists)) {
    print "creating VIEW_MAIN: $header_main $statement_main $footer_main\n";
    $dbh->do("$header_main $statement_main $footer_main");
  }
  $sth = $dbh->prepare("SELECT TABLE_NAME FROM information_schema.columns WHERE table_schema = '$db' GROUP BY TABLE_NAME");
  $sth->execute;
  while (my $ref = $sth->fetchrow_hashref()) {
    # CREATE VIEW `User1Videos93`.`actor_link` AS SELECT * FROM `MyVideos93`.`actor_link`;
    if (!(($ref->{'TABLE_NAME'} eq "files") || ($ref->{'TABLE_NAME'} eq "bookmark"))) {
      push @tables, $ref->{'TABLE_NAME'};
    }
  }
  foreach (@tables) {
    $table = $_;
    $exists = &checkifexists("${target}${rev}","$table");
    if (!($exists)) {
      &checkdb("${target}${rev}");
      if (!($table ~~ @views)) {
        $dbh->do("CREATE VIEW `${target}${rev}`.`$table` AS SELECT * FROM `$db`.`$table`");
      }
    }
  }
}

sub checkslavecol {
  my ($target,$highrev) = @_;
  print "TARGET: $target HIGH REV: $highrev\n";
  $exists = &checkifexists("${target}$highrev","bookmark");
  if (!($exists)) {
    # could probably make this table dynamic instead of this.. basically just recreate the table; updates might be tricky like comparing the new and old tables of the main db
    # and figuring out what was added after copying -- needs investigating, and need to see if this table ever changes and if so, how often.
    $dbh->do("CREATE TABLE `${target}$highrev`.`bookmark` ( idBookmark integer primary key AUTO_INCREMENT, idFile integer, timeInSeconds double, totalTimeInSeconds double, thumbNailImage text, player text, playerState text, type integer)");
    $dbh->do("USE ${target}$highrev");
    $dbh->do("CREATE INDEX ix_bookmark ON bookmark (idFile, type);");
  }
}

sub createviewhash {
  my ($highdb,$target,$highrev) = @_;
  my @views_order;
  # get a list of the views in the video DB from the highest revision
  $sth = $dbh->prepare("SHOW FULL TABLES IN $highdb WHERE TABLE_TYPE LIKE 'VIEW'");
  $sth->execute;
  $dbh->do("USE ${target}$highrev");
  while (my $ref = $sth->fetchrow_hashref()) {
    my $view = $ref->{"Tables_in_$highdb"};
    push @views, $view;
  }
  foreach (@views) {
    my $view = $_;
    print "Fetching create info for $_\n";
    $sth = $dbh->prepare("SHOW CREATE VIEW `$highdb`.`$view`");
    $dbh->do("USE ${target}$highrev");
    $sth->execute;
    my $statement;
    while (my $ref = $sth->fetchrow_hashref()) {
      $statement = $ref->{'Create View'};
    }
    # change all the dbs to the target.
    ($statement = $statement) =~ s/$highdb/${target}$highrev/g;
    ($statement = $statement) =~ s/${MAINDB}/${target}/g;
    print "HIGHDB: $highdb TARGET: $target MAINDB: $MAINDB\n";
    print "$statement\n";
    # remove the security statements from the CREATE.
    ($statement = $statement) =~ s/^CREATE .* VIEW `${target}$highrev`.`$view`/CREATE VIEW `${target}$highrev`.`$view`/g;
    # whenever we see files, make sure we use globalfiles from the master database instead
    #($statement = $statement) =~ s/`${target}_Videos$highrev`.`globalfiles`/`$highdb`.`globalfiles`/g;
    foreach (@file_exceptions) {
      my $exc = $_;
      # make sure we change the exceptions (playCount, lastPlayed for now) changed to the target ones.
      #($statement = $statement) =~ s/`${target}_Videos$highrev`.`files`.`$exc`/`$highdb`.`files`.`${target}_$exc`/;
    }
    $view_hash{$view}{'statement'} = "$statement";
    foreach (@views) {
      my $testview = $_;
      if ($statement =~ m/SELECT .*`$testview`/i) {
        print "view $view requires $testview\n";
        push @{$view_hash{$view}{'dep'}}, $testview;
      }
    }
  }
}

sub recreatehighviews {
  my ($target,$highrev) = @_;
  foreach (keys %view_hash) {
    &checkdeps($_,$target,$highrev);
  }
}

sub checkdeps {
  my $exists;
  my ($view,$target,$highrev) = @_;
  print "view check: $view -- $target -- $highrev\n";
  my $depview;
  if ($view_hash{$view}{'dep'}) {
    foreach (@{$view_hash{$view}{'dep'}}) {
      $depview = $_;
      if (!($view_hash{$depview}{'pushed'})) {
        print "Need to check $depview, missing dep\n";
        &checkdeps($depview,$target,$highrev);
      }
    }
    if (!($view_hash{$view}{'pushed'}) == 1) {
      $exists = &checkifexists("${target}$highrev","$view");
      if (!($exists)) {
        $dbh->do("$view_hash{$view}{'statement'}");
      }
      $view_hash{$depview}{'pushed'} = 1;
    } else {
      print "$view already pushed\n";
    }
  } else {
    if (!($view_hash{$view}{'pushed'} == 1)) {
      $exists = &checkifexists("${target}$highrev","$view");
      if (!($exists)) {
        $dbh->do("$view_hash{$view}{'statement'}");
      }
      $view_hash{$view}{'pushed'} = 1;
    } else {
      print "$view already pushed\n";
    }
  }
}

&main;
