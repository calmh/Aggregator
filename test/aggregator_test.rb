require 'test/unit'
require 'aggregator'

$:.unshift File.dirname(__FILE__)
require 'test_helper'

class TestAggregator < Test::Unit::TestCase
  def setup
    # The class under inspection shall have no secrets
    @privates = Aggregator.publicize_methods

    db_setup if ENV['HAVE_LOCAL_DB']
  end

  def teardown
    db_teardown if ENV['HAVE_LOCAL_DB']

    Aggregator.privatize_methods @privates
  end

  def test_numeric_must_respond_to_minute
    assert_equal(60, 1.minute)
    assert_equal(60, 1.minutes)
  end

  def test_numeric_must_respond_to_hour
    assert_equal(3600, 1.hour)
    assert_equal(3600, 1.hours)
  end

  def test_numeric_must_respond_to_day
    assert_equal(86400, 1.day)
    assert_equal(86400, 1.days)
  end

  def test_numeric_must_respond_to_week
    assert_equal(7*86400, 1.week)
    assert_equal(7*86400, 1.weeks)
  end

  def test_numeric_must_respond_to_month
    assert_equal(30*86400, 1.month)
    assert_equal(30*86400, 1.months)
  end

  def test_numeric_must_respond_to_year
    assert_equal(365*86400, 1.year)
    assert_equal(365*86400, 1.years)
  end

  def test_numeric_must_respond_to_ago
    now = Time.new.to_f
    assert_in_delta(now - 365*86400, 1.year.ago, 1.0)
  end

  def test_aggregator_must_respond_to_rules_and_default_to_empty_array
    ag = Aggregator.new
    assert_equal([], ag.rules)
  end

  def test_aggregator_must_respond_to_index_and_default_to_true
    ag = Aggregator.new
    assert_equal(true, ag.index)
  end

  def test_aggregator_must_respond_to_optimize_and_default_to_true
    ag = Aggregator.new
    assert_equal(true, ag.optimize)
  end

  def test_aggregator_must_respond_to_runlimit_and_default_to_nil
    ag = Aggregator.new
    assert_equal(nil, ag.runlimit)
  end

  def test_aggregator_must_respond_to_database_and_default_to_nil
    ag = Aggregator.new
    assert_equal(nil, ag.database)
  end

  def test_aggregator_must_respond_to_reaggregate_interval_and_default_to_1_year
    ag = Aggregator.new
    assert_equal(1.year, ag.reaggregate_interval)
  end

  def test_aggregator_should_return_sorted_rules
    ag = Aggregator.new
    ag.rules << { :age => 1.month }
    ag.rules << { :age => 1.year }
    ag.rules << { :age => 1.day }
    assert_equal(1.year, ag.rules[0][:age])
    assert_equal(1.day, ag.rules[2][:age])
  end

  def test_aggregator_should_find_rules_for_table
    ag = Aggregator.new
    ag.rules << { :table => :all, :age => 1.month, :reduce => 1.hour }
    ag.rules << { :table => "foo", :age => 2.month, :reduce => 2.hour }
    ag.rules << { :table => "bar", :age => 2.month, :reduce => 6.hour }
    ag.rules << { :table => /foo/, :age => 3.month, :reduce => 3.hour }
    ag.rules << { :table => /bar/, :age => 3.month, :reduce => 12.hour }
    list = ag.rules_for "foo"
    assert_equal(3, list.length)
    assert_equal(3.hour, list[0][:reduce])
    assert_equal(2.hour, list[1][:reduce])
    assert_equal(1.hour, list[2][:reduce])
  end

  def test_aggregator_more_specifics_should_decide
    ag = Aggregator.new
    ag.rules << { :table => :all, :age => 1.month, :reduce => 1.hour }
    ag.rules << { :table => :all, :age => 2.month, :reduce => 2.hour }
    ag.rules << { :table => /bar/, :age => 2.month, :reduce => 4.hour }
    ag.rules << { :table => /bar/, :age => 4.month, :reduce => 8.hour }
    ag.rules << { :table => "bar", :age => 4.month, :drop => true }
    list = ag.rules_for "bar"
    assert_equal(3, list.length)
    assert_equal(true, list[0][:drop])
    assert_equal(4.hour, list[1][:reduce])
    assert_equal(1.hour, list[2][:reduce])
  end

  def test_aggregator_must_respond_excludes_and_default_to_some_array
    ag = Aggregator.new
    assert_kind_of(Array, ag.excludes)
    assert_not_same(0, ag.excludes.length)
  end

  def test_aggregator_must_exclude_standard_tables
    ag = Aggregator.new
    assert(ag.exclude? "pruned")
    assert(ag.exclude? "routers")
    assert(ag.exclude? "interfaces")
  end

  def test_aggregator_must_exclude_custom_tables
    ag = Aggregator.new
    ag.excludes << /^dlink/
    assert(ag.exclude?("dlinkCpuPercent_1234"))
  end

  def test_aggregator_must_not_exclude_random_tables
    ag = Aggregator.new
    assert(!ag.exclude?("dlinkCpuPercent_1234"))
  end

  def test_aggregator_must_not_connect_without_database
    ag = Aggregator.new
    assert_raises Mysql::Error do
      ag.connection
    end
  end

  def test_aggregator_must_not_connect_with_bad_database
    ag = Aggregator.new
    ag.database = { :host => "db.example.com", :user => "aggregator", :password => "abc123", :database => "rtg" }
    assert_raises Mysql::Error do
      ag.connection
    end
  end

  def test_aggregator_must_connect_to_local_db
    return unless ENV['HAVE_LOCAL_DB']
    ag = Aggregator.new
    ag.database = { :host => TESTDBHOST, :user => TESTDBUSER, :password => TESTDBPASS, :database => TESTDBDATABASE }
    assert_not_equal(nil, ag.connection)
  end

  def test_aggregator_must_return_one_table_in_list
    return unless ENV['HAVE_LOCAL_DB']
    ag = Aggregator.new
    ag.database = { :host => TESTDBHOST, :user => TESTDBUSER, :password => TESTDBPASS, :database => TESTDBDATABASE }
    tables = ag.tables
    assert_equal(1, tables.length)
    assert_equal("ifInOctets_252", tables[0])
  end

  def test_aggregator_should_set_and_get_pruned
    return unless ENV['HAVE_LOCAL_DB']
    ag = Aggregator.new
    ag.database = { :host => TESTDBHOST, :user => TESTDBUSER, :password => TESTDBPASS, :database => TESTDBDATABASE }
    ag.set_pruned 'foo'
    assert_kind_of(DateTime, ag.get_pruned('foo'))
  end

  def test_aggregator_should_get_table_ids
    return unless ENV['HAVE_LOCAL_DB']
    ag = Aggregator.new
    ag.database = { :host => TESTDBHOST, :user => TESTDBUSER, :password => TESTDBPASS, :database => TESTDBDATABASE }
    ids = ag.ids 'ifInOctets_252'
    assert_equal(1, ids.length)
    assert_equal(42, ids[0])
  end

  def test_aggregator_should_get_table_rows
    return unless ENV['HAVE_LOCAL_DB']
    ag = Aggregator.new
    ag.database = { :host => TESTDBHOST, :user => TESTDBUSER, :password => TESTDBPASS, :database => TESTDBDATABASE }
    rows = ag.rows 'ifInOctets_252', 42, 2.month.ago, 1.month.ago
    assert_in_delta(50, rows.length, 5)
  end

  def test_aggregator_must_recognize_counters_example_one
    rows = []
    rows << [   0, 300*1e6, 1e6 ]
    rows << [ 300, 300*1e6, 1e6 ]
    rows << [ 600, 300*1e6, 1e6 ]

    ag = Aggregator.new
    assert(!ag.gauge?(rows))
  end

  def test_aggregator_must_recognize_counters_example_two
    rows = []
    rows << [   0, 300*1.0e6, 1.0e6 ]
    rows << [ 300, 300*2.0e6, 2.0e6 ]
    rows << [ 600, 300*1.5e6, 1.5e6 ]

    ag = Aggregator.new
    assert(!ag.gauge?(rows))
  end

  def test_aggregator_must_recognize_gauge
    rows = []
    rows << [   0, 100, 100 ]
    rows << [ 300, 100, 100 ]
    rows << [ 600, 100, 100 ]

    ag = Aggregator.new
    assert(ag.gauge?(rows))
  end

  def test_aggregator_should_consider_table_as_gauge_always_with_string
    ag = Aggregator.new
    ag.always_gauge << "bargle"
    ag.always_gauge << "foo"
    assert(!ag.configured_as_gauge?("bar"))
    assert(ag.configured_as_gauge?("foo"))
  end

  def test_aggregator_should_consider_table_as_gauge_always_with_regexp
    ag = Aggregator.new
    ag.always_gauge << "bargle"
    ag.always_gauge << /oo/
    assert(!ag.configured_as_gauge?("bar"))
    assert(ag.configured_as_gauge?("foo"))
  end

  def test_aggregator_should_aggregate_counter_rows
    base = 1234567890
    rows, counter_sum, rate_avg = counter_rows(100, base, 1e6, 300)
    base += 99*300;
    ag = Aggregator.new
    rows = ag.summary_row rows
    assert_equal(3, rows.length) # One array with three values
    assert_equal(base, rows[0]) # Last time represents all rows
    assert_equal(counter_sum, rows[1]) # New counter_diff is sum of all counter_diffs
    assert_equal(rate_avg, rows[2]) # New rate is average of all rates
  end

  def test_aggregator_should_aggregate_gauge_rows
    base = 1234567890
    rows, rate_avg = gauge_rows(100, base, 1e6, 300)
    base += 99*300;

    ag = Aggregator.new
    rows = ag.summary_row rows
    assert_equal(3, rows.length) # One array with three values
    assert_equal(base, rows[0]) # Last time represents all rows
    assert_equal(rate_avg, rows[1]) # New counter_diff is average of all counter_diffs
    assert_equal(rate_avg, rows[2]) # New rate is average of all rates
  end

  def test_aggregator_should_cluster_rows
    ag = Aggregator.new
    rows, counter_sum, rate_avg = counter_rows(100, 10, 1e6, 300)
    clustered = ag.cluster_rows rows, 3600
    assert_equal(9, clustered.length)
    assert_equal(12, clustered[0].length)
    assert_equal(12, clustered[7].length)
    assert_equal(4, clustered[8].length)
  end

  def test_aggregator_should_aggregate_table_by_hour
    return unless ENV['HAVE_LOCAL_DB']
    ag = Aggregator.new
    ag.database = { :host => TESTDBHOST, :user => TESTDBUSER, :password => TESTDBPASS, :database => TESTDBDATABASE }
    queries = ag.aggregate 'ifInOctets_252', 42, 2.month.ago, 1.month.ago, 1.hour
    assert(queries.length > 2)
    assert(queries[0] =~ /DELETE FROM ifInOctets_252 WHERE id = 42 AND/)
    assert(queries[1] =~ /INSERT INTO ifInOctets_252 \(id, dtime, counter, rate\) VALUES \(/)
  end

  def test_aggregator_should_aggregate_table_by_hour_and_not_repeat_itself
    return unless ENV['HAVE_LOCAL_DB']
    ag = Aggregator.new
    ag.database = { :host => TESTDBHOST, :user => TESTDBUSER, :password => TESTDBPASS, :database => TESTDBDATABASE }
    queries = ag.aggregate 'ifInOctets_252', 42, 2.month.ago, 1.month.ago, 1.hour
    assert(queries.length > 2)
    assert(queries[0] =~ /DELETE FROM ifInOctets_252 WHERE id = 42 AND/)
    assert(queries[1] =~ /INSERT INTO ifInOctets_252 \(id, dtime, counter, rate\) VALUES \(/)
    # Execute the suggested queries
    queries.each { |q| conn.query(q) }
    # Verify that a new aggregations finds nothing to do.
    queries = ag.aggregate 'ifInOctets_252', 42, 2.month.ago, 1.month.ago, 1.hour
    assert_equal(0, queries.length)
  end

  def test_aggregator_should_run
    return unless ENV['HAVE_LOCAL_DB']
    assert_nothing_raised do
      ag = Aggregator.new
      ag.database = { :host => TESTDBHOST, :user => TESTDBUSER, :password => TESTDBPASS, :database => TESTDBDATABASE }
      ag.rules << { :table => :all, :age => 14.day, :reduce => 1.hour }
      ag.rules << { :table => :all, :age => 1.month, :reduce => 2.hour  }
      ag.rules << { :table => :all, :age => 2.month, :drop => true }
      ag.verbose = false
      ag.runlimit = 50.minute
      ag.run
    end
  end

  def test_aggregator_should_aggregate_no_rows_without_exception
    return unless ENV['HAVE_LOCAL_DB']

    assert_nothing_raised do
      ag = Aggregator.new
      ag.database = { :host => TESTDBHOST, :user => TESTDBUSER, :password => TESTDBPASS, :database => TESTDBDATABASE }
      ag.rules << { :table => :all, :age => 2.year, :reduce => 1.hour }
      ag.verbose = false
      ag.runlimit = 50.minute
      ag.run
    end
  end

  def test_aggregator_should_run_new_syntax
    return unless ENV['HAVE_LOCAL_DB']
    assert_nothing_raised do
      Aggregator.aggregate do |ag|
        ag.database = { :host => TESTDBHOST, :user => TESTDBUSER, :password => TESTDBPASS, :database => TESTDBDATABASE }
        ag.rules << { :table => :all, :age => 14.day, :reduce => 1.hour }
        ag.rules << { :table => :all, :age => 1.month, :reduce => 2.hour  }
        ag.rules << { :table => :all, :age => 2.month, :drop => true }
        ag.verbose = false
        ag.runlimit = 50.minute
      end
    end
  end

  def test_aggregator_should_create_pruned_table
    return unless ENV['HAVE_LOCAL_DB']
    a = Aggregator.new
    a.create_pruned_table(conn)
    assert(conn.list_tables.include? 'pruned')
  end

  def test_aggregator_should_create_pruned_table_or_exit_cleanly
    return unless ENV['HAVE_LOCAL_DB']
    a = Aggregator.new
    a.create_pruned_table(conn)
    assert(conn.list_tables.include? 'pruned')
    assert_nothing_raised do
      a.create_pruned_table(conn)
    end
  end

  def test_get_pruned_should_return_nil
    return unless ENV['HAVE_LOCAL_DB']
    a = Aggregator.new
    a.database = { :host => TESTDBHOST, :user => TESTDBUSER, :password => TESTDBPASS, :database => TESTDBDATABASE }
    gp = a.get_pruned('ifInOctets_252')
    assert_equal(nil, gp)
  end

  def test_needs_pruning_should_return_true
    return unless ENV['HAVE_LOCAL_DB']
    a = Aggregator.new
    a.database = { :host => TESTDBHOST, :user => TESTDBUSER, :password => TESTDBPASS, :database => TESTDBDATABASE }
    a.rules << { :table => :all, :age => 14.day, :reduce => 1.hour }
    np = a.needs_pruning?('ifInOctets_252')
    assert_equal(true, np)
  end

  def test_needs_pruning_should_return_false
    return unless ENV['HAVE_LOCAL_DB']
    a = Aggregator.new
    a.database = { :host => TESTDBHOST, :user => TESTDBUSER, :password => TESTDBPASS, :database => TESTDBDATABASE }
    a.rules << { :table => :all, :age => 14.day, :reduce => 1.hour }
    a.set_pruned('ifInOctets_252')
    np = a.needs_pruning?('ifInOctets_252')
    assert_equal(false, np)
  end
end
