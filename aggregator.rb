require 'mysql'
require 'date'

# Define some convenient shorthands for specifying times
class Numeric
	def minute ; self * 60 ; end
	def hour ; self * 3600 ; end
	def day ; self * 86400 ; end
	def week ; 7 * day ; end
	def month ; 30 * day ; end
	def year ; 365 * day ; end
	def ago ; (Time.new.to_f - self).to_i ; end
end

class Aggregator
	# Rules for handling tables. Add hashes of that include the keys:
	#  :age    # Rule triggers for rows that are at least this old
	#  :table  # Which tables to use this rule on. Use a regexp or symbol :all.
	#  :reduce # Reduce data to one hour averages
	#  :drop   # Don't reduce, just drop older data
	# Reduce data older than one year to eight hour averages, for all tables:
	#  rules << { :table => :all, :age => 1.year, :reduce => 8.hour }
	# Drop data older than two years from ADSL tables:
	#  rules << { :table => /^adsl/, :age => 2.year, :drop => true }
	attr_accessor :rules
	# Whether or not to index tables before optimization.
	# If this is set to true (default), indexes will be created, if they are missing,
	# on dtime and id. This is recommended for acceptable aggregation performance.
	#  index = true
	attr_accessor :index
	# Whether or not to run OPTIMIZE TABLE after aggregation.
	# Defaults to true.
	#  optimize = true
	attr_accessor :optimize
	# List of tables to exclude. Accepts literal strings (exakt match) and
	# regular expressions. By default certain non time-series tables of RTG.
	# such as +interfaces+ and +routers+ are excluded. If you have custom
	# tables that should not be touched by aggregator, add them here.
	#  excludes << "custom_table"  # Don't touch "custom_table"
	#  excludes << /^dlink/        # Don't touch tables whose name starts with "dlink"
	attr_accessor :excludes
	# Limit run time of aggregator to a certain time span.
	# This may be useful if aggregator is begin run from cron at a
	# certain interval, to avoid overlapping processes.
	# Set to nil (default) to no limit the run time.
	# Note that aggregator always completes a table it has started processing, so
	# the specified runtime might get exceeded.
	#  runlimit = nil         # No limit
	#  runlimit = 50.minutes # Almost guaranteed to be done in an hour.
	attr_accessor :runlimit
	# How ofter to reaggregate a table. Defaults to one month.
	#  reaggregate_interval = 1.month
	attr_accessor :reaggregate_interval
	# How to access the RTG database. Accepts a hash containing connection parameters.
	#  database = { :host => "localhost", :user => "aggregator", :password => "abc123", :database => "rtg" }
	attr_accessor :database
	# Whether to do any altering operations on the database or not. If set to false, nothing will be done
	# and all altering SQL queries will be echoed to stdout instead. This might fail, if for example the
	# pruned table doesn't exist.
	#  dry_run = false
	attr_accessor :dry_run
	attr_accessor :verbose

	def initialize
		@rules = []
		@index = true
		@optimize = true
		@excludes = [ 'interfaces', 'routers', 'pruned' ]
		@runlimit = nil
		@reaggregate_interval = 1.year
		@database = nil
		@dry_run = false
		@verbose = false
	end

	# List of all currently active rules.
	def rules
		# Rules should always be returned sorted with oldest first.
		@rules.sort! { |a, b| b[:age] <=> a[:age] }
	end

	# Create SQL commands for aggregating a table/id combination between the specified times to the specified interval.
	# Results in an array of DELETE and INSERT commands.
	def aggregate(table, id, start_time, end_time, interval)
		rows = rows(table, id, start_time, end_time)
		return [] if rows.count < 2
		queries = []
		clusters = cluster_rows(rows, interval)
		clusters.each do |cluster|
			if cluster.length > 1
				summary = summary_row(cluster)
				insert = "INSERT INTO #{table} (id, dtime, counter, rate) VALUES (#{id}, FROM_UNIXTIME(#{summary[0]}), #{summary[1]}, #{summary[2]})"
				delete = "DELETE FROM #{table} WHERE id = #{id} AND dtime IN (" + cluster.collect{ |c| "FROM_UNIXTIME(#{c[0]})" }.join(", ") + ")"
				queries << delete << insert
			end
		end
		return queries
	end

	def drop(table, end_time)
		"DELETE FROM #{table} WHERE dtime <= FROM_UNIXTIME(#{end_time})"
	end

	def run
		to_process = tables
		processing_limit_time = Time.new + runlimit
		while to_process.length > 0 && (runlimit.nil? || Time.new < processing_limit_time)
			table = to_process.delete_at(rand(to_process.length))
			last_pruned = get_pruned(table)
			next if !last_pruned.nil? && DateTime.now < last_pruned + reaggregate_interval / 86400.0

			table_rules = rules_for(table)
			next if table_rules.length == 0

			puts "Looking at #{table}" if @verbose

			if @index
				create_indexes(table)
			end

			queries = []
			prev_rule_end_time = nil
			table_rules.each do |rule|
				end_time = rule[:age].ago
				if rule.key? :drop
					queries << drop(table, end_time)
				elsif rule.key? :reduce
					interval = rule[:reduce]
					start_time = prev_rule_end_time || 0
					ids(table).each do |id|
						queries += aggregate(table, id, start_time, end_time, interval)
					end
				end
				prev_rule_end_time = end_time
			end

			deletes = 0
			inserts = 0
			if !@dry_run
				connection.query("SET AUTOCOMMIT=0")
				connection.query("BEGIN")
				queries.each do |q|
					connection.query(q)
					if q =~ /INSERT/
						inserts += connection.affected_rows
					elsif q =~ /DELETE/
						deletes += connection.affected_rows
					end
				end
				connection.query("COMMIT")
				connection.query("SET AUTOCOMMIT=1")
			else
				queries.each { |q| puts q } if @verbose
			end

			puts "  Inserts: #{inserts}" if @verbose
			puts "  Deletes: #{deletes}" if @verbose

			if @optimize
				optimize_table(table)
			end

			set_pruned(table)

			puts "  #{to_process.length} tables left to handle." if @verbose
		end
	end

	private

	#
	# Logic stuff
	#

	def cluster_rows(rows, interval)
		clusters = []
		cur_cluster = []
		interval_end = (rows[0][0] / interval + 1).to_i * interval
		rows.each do |row|
			if row[0] > interval_end
				clusters << cur_cluster if cur_cluster.length > 0
				cur_cluster = []
				interval_end += interval
			end
			cur_cluster << row
		end
		clusters << cur_cluster if cur_cluster.length > 0
		return clusters
	end

	# Check if the named table should be excluded.
	def exclude?(table)
		@excludes.each do |excl|
			if excl.kind_of? String
				return true if table == excl
			elsif excl.kind_of? Regexp
				return true if table =~ excl
			end
		end
		return false
	end

	# Return a list of rules that applies to the specified table.
	def rules_for(table)
		for_everyone = @rules.select { |rule| rule[:table] == :all }
		for_me_regexp = @rules.select { |rule| rule[:table] != :all && rule[:table].kind_of?(Regexp) && table =~ rule[:table] }
		for_me_string = @rules.select { |rule| rule[:table] != :all && rule[:table].kind_of?(String) && table == rule[:table] }
		for_me_string.each do |r|
			for_me_regexp = for_me_regexp.select { |nr| nr[:age] != r[:age] }
			for_everyone = for_everyone.select { |nr| nr[:age] != r[:age] }
		end
		for_me_regexp.each do |r|
			for_everyone = for_everyone.select { |nr| nr[:age] != r[:age] }
		end
		valid_rules = for_everyone + for_me_regexp + for_me_string
		valid_rules.sort { |a, b| b[:age] <=> a[:age] }
	end

	def summary_row(rows)
		return nil if rows.nil? || rows.length == 0
		return rows[0] if rows.length == 1
		last_row = rows[rows.length-1]
		if gauge? rows
			average = rows.inject(0) { |m, n| m += n[2] } / rows.length
			return [ last_row[0], average, average ]
		else
			average_rate = rows.inject(0) { |m, n| m += n[2] } / rows.length
			counter_sum = rows.inject(0) { |m, n| m += n[1] }
			return [ last_row[0], counter_sum, average_rate ]
		end
	end

	def gauge?(rows)
		required_gauge_confidence = 2
		probably_gauge = 0
		rows = rows.dup
		while rows.length > 0 && probably_gauge >= 0 && probably_gauge < required_gauge_confidence
			row = rows.delete_at(rand(rows.length))
			probably_gauge += 1 if row[1] == row[2]
		end
		return probably_gauge >= required_gauge_confidence
	end

	#
	# Pruning stuff
	#

	# Verify that we have a pruned table, or create it if not.
	def create_pruned_table(conn)
		if !@dry_run && !conn.list_tables.include?('pruned')
			res = conn.query("CREATE TABLE `pruned` (
			`table_name` VARCHAR(64) NOT NULL PRIMARY KEY,
			`prune_time` DATETIME NOT NULL
			)")
		end
	end

	# Get the latest prune time for specified table, or nil if never
	def get_pruned(table)
		res = connection.query("SELECT prune_time FROM pruned WHERE table_name = '#{table}'")
		if res.num_rows == 1
			row = res.fetch_row
			return DateTime.parse(row[0])
		end
		return nil
	end

	# Mark the specified table as pruned
	def set_pruned(table)
		if !@dry_run
			res = connection.query("UPDATE pruned SET prune_time = now() WHERE table_name = '#{table}'")
			if connection.affected_rows == 0
				connection.query("INSERT INTO pruned (table_name, prune_time) VALUES ('#{table}', now())")
			end
			puts "  Updated prune_time." if @verbose
		end
	end

	#
	# Database stuff
	#

	# Get a database connection or raise an error if we can't
	def connection
		raise Mysql::Error, "Cannot connect without database information" if @database.nil?
		if !@conn
			@conn = Mysql::new(@database[:host], @database[:user], @database[:password], @database[:database])
			create_pruned_table(@conn)
		end
		@conn
	end

	# Return a list of all non-excluded tables
	def tables
		connection.list_tables.select { |t| !exclude?(t) }
	end

	# Return a list of all ids in a table.
	def ids(table)
		res = connection.query("SELECT id FROM #{table} GROUP BY id")
		id_list = []
		res.each { |i| id_list << i[0].to_i }
		return id_list
	end

	# Return a list of all ids in a table.
	def rows(table, id, start_time, end_time)
		res = connection.query("SELECT UNIX_TIMESTAMP(dtime), counter, rate FROM #{table} WHERE id = #{id} AND dtime >= FROM_UNIXTIME(#{start_time}) AND dtime <= FROM_UNIXTIME(#{end_time})")
		rows = []
		res.each { |i| rows << [ i[0].to_i, i[1].to_i, i[2].to_i ] }
		return rows
	end

	# Create necessary indexes, ignoring exceptions if they already exist.
	def create_indexes(table)
		if !@dry_run
			begin
				connection.query("CREATE INDEX id_idx ON #{table} (id)")
				puts "  Created index id_idx." if @verbose
			rescue; end

			begin
				connection.query("CREATE INDEX #{table}_idx ON #{table} (dtime)")
				puts "  Created index #{table}_idx." if @verbose
			rescue; end
		end
	end

	# Optimize table
	def optimize_table(table)
		if !@dry_run
			connection.query("OPTIMIZE TABLE #{table}")
			puts "  Optimized table." if @verbose
		end
	end
end

if __FILE__ == $PROGRAM_NAME
	require 'test/unit'
	class TestAggregator < Test::Unit::TestCase
		HAVE_LOCAL_DB = true
		TESTDBHOST = 'localhost'
		TESTDBUSER = 'root'
		TESTDBPASS = nil
		TESTDBDATABASE = 'rtg'

		def conn
			@conn ||= Mysql::new(TESTDBHOST, TESTDBUSER, TESTDBPASS, nil)
		end

		def counter_rows(num, base, rate, interval)
			rows = []
			counter_sum = 0
			rate_sum = 0
			num.times do |i|
				rate_sum += rate
				counter_sum += rate*interval
				rows << [ base, rate*interval, rate ]
				rate = rate * 1.1
				base += interval
			end
			rate_avg = rate_sum / num
			return [rows, counter_sum, rate_avg]
		end

		def gauge_rows(num, base, rate, interval)
			rows = []
			rate_sum = 0
			num.times do |i|
				rate_sum += rate
				rows << [ base, rate, rate ]
				rate = rate * 1.1
				base += interval
			end
			rate_avg = rate_sum / num
			return [rows, rate_avg]
		end

		def setup
			if HAVE_LOCAL_DB
				conn.query("CREATE DATABASE #{TESTDBDATABASE}")
				conn.query("USE #{TESTDBDATABASE}")
				conn.query("CREATE TABLE `interfaces` ( `id` int )")
				conn.query("CREATE TABLE `routers` ( `id` int )")
				conn.query("CREATE TABLE `ifInOctets_252` (
				`id` int(11) NOT NULL,
				`dtime` datetime NOT NULL,
				`counter` bigint(20) NOT NULL,
				`rate` bigint(20) default NULL
				)")
				insert = "INSERT INTO `ifInOctets_252` ( `id`, `dtime`, `counter`, `rate` ) VALUES "
				base = (1.month + 4.hour).ago
				bps = 1e6
				inc = 0.1e6
				insert += 1.upto(100).map { |i| "( 42, FROM_UNIXTIME(#{base + i * 300}), #{(bps + inc * i) * 8 * 300}, #{(bps + inc * i) * 8} )"  }.join(", ")
				conn.query(insert)
			end
		end

		def teardown
			if HAVE_LOCAL_DB
				conn.query("DROP DATABASE #{TESTDBDATABASE}")
			end
		end

		def test_numeric_must_respond_to_minute
			assert_equal(60, 1.minute)
		end

		def test_numeric_must_respond_to_hour
			assert_equal(3600, 1.hour)
		end

		def test_numeric_must_respond_to_day
			assert_equal(86400, 1.day)
		end

		def test_numeric_must_respond_to_week
			assert_equal(7*86400, 1.week)
		end

		def test_numeric_must_respond_to_month
			assert_equal(30*86400, 1.month)
		end

		def test_numeric_must_respond_to_year
			assert_equal(365*86400, 1.year)
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

		def test_aggregator_must_respond_to_reaggregate_interval_and_default_to_1_month
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
			list = ag.send(:rules_for, "foo")
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
			list = ag.send(:rules_for, "bar")
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
			assert(ag.send(:exclude?, "pruned"))
			assert(ag.send(:exclude?, "routers"))
			assert(ag.send(:exclude?, "interfaces"))
		end

		def test_aggregator_must_exclude_custom_tables
			ag = Aggregator.new
			ag.excludes << /^dlink/
			assert(ag.send(:exclude?, "dlinkCpuPercent_1234"))
		end

		def test_aggregator_must_not_exclude_random_tables
			ag = Aggregator.new
			assert(!ag.send(:exclude?, "dlinkCpuPercent_1234"))
		end

		def test_aggregator_must_not_connect_without_database
			ag = Aggregator.new
			assert_raises Mysql::Error do
				ag.send(:connection)
			end
		end

		def test_aggregator_must_not_connect_with_bad_database
			ag = Aggregator.new
			ag.database = { :host => "db.example.com", :user => "aggregator", :password => "abc123", :database => "rtg" }
			assert_raises Mysql::Error do
				ag.send(:connection)
			end
		end

		def test_aggregator_must_connect_to_local_db
			if HAVE_LOCAL_DB
				ag = Aggregator.new
				ag.database = { :host => TESTDBHOST, :user => TESTDBUSER, :password => TESTDBPASS, :database => TESTDBDATABASE }
				assert_not_equal(nil, ag.send(:connection))
			end
		end

		def test_aggregator_must_return_one_table_in_list
			if HAVE_LOCAL_DB
				ag = Aggregator.new
				ag.database = { :host => TESTDBHOST, :user => TESTDBUSER, :password => TESTDBPASS, :database => TESTDBDATABASE }
				tables = ag.send(:tables)
				assert_equal(1, tables.length)
				assert_equal("ifInOctets_252", tables[0])
			end
		end

		def test_aggregator_should_set_and_get_pruned
			if HAVE_LOCAL_DB
				ag = Aggregator.new
				ag.database = { :host => TESTDBHOST, :user => TESTDBUSER, :password => TESTDBPASS, :database => TESTDBDATABASE }
				ag.send(:set_pruned, 'foo')
				assert_kind_of(DateTime, ag.send(:get_pruned, 'foo'))
			end
		end

		def test_aggregator_should_get_table_ids
			if HAVE_LOCAL_DB
				ag = Aggregator.new
				ag.database = { :host => TESTDBHOST, :user => TESTDBUSER, :password => TESTDBPASS, :database => TESTDBDATABASE }
				ids = ag.send(:ids, 'ifInOctets_252')
				assert_equal(1, ids.length)
				assert_equal(42, ids[0])
			end
		end

		def test_aggregator_should_get_table_rows
			if HAVE_LOCAL_DB
				ag = Aggregator.new
				ag.database = { :host => TESTDBHOST, :user => TESTDBUSER, :password => TESTDBPASS, :database => TESTDBDATABASE }
				rows = ag.send(:rows, 'ifInOctets_252', 42, 2.month.ago, 1.month.ago)
				assert_in_delta(50, rows.length, 5)
			end
		end

		def test_aggregator_must_recognize_counters_example_one
			rows = []
			rows << [   0, 300*1e6, 1e6 ]
			rows << [ 300, 300*1e6, 1e6 ]
			rows << [ 600, 300*1e6, 1e6 ]

			ag = Aggregator.new
			assert(!ag.send(:gauge?, rows))
		end

		def test_aggregator_must_recognize_counters_example_two
			rows = []
			rows << [   0, 300*1.0e6, 1.0e6 ]
			rows << [ 300, 300*2.0e6, 2.0e6 ]
			rows << [ 600, 300*1.5e6, 1.5e6 ]

			ag = Aggregator.new
			assert(!ag.send(:gauge?, rows))
		end

		def test_aggregator_must_recognize_gauge
			rows = []
			rows << [   0, 100, 100 ]
			rows << [ 300, 100, 100 ]
			rows << [ 600, 100, 100 ]

			ag = Aggregator.new
			assert(ag.send(:gauge?, rows))
		end

		def test_aggregator_should_aggregate_counter_rows
			base = 1234567890
			rows, counter_sum, rate_avg = counter_rows(100, base, 1e6, 300)
			base += 99*300;
			ag = Aggregator.new
			rows = ag.send(:summary_row, rows)
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
			rows = ag.send(:summary_row, rows)
			assert_equal(3, rows.length) # One array with three values
			assert_equal(base, rows[0]) # Last time represents all rows
			assert_equal(rate_avg, rows[1]) # New counter_diff is average of all counter_diffs
			assert_equal(rate_avg, rows[2]) # New rate is average of all rates
		end

		def test_aggregator_should_cluster_rows
			ag = Aggregator.new
			rows, counter_sum, rate_avg = counter_rows(100, 10, 1e6, 300)
			clustered = ag.send(:cluster_rows, rows, 3600)
			assert_equal(9, clustered.length)
			assert_equal(12, clustered[0].length)
			assert_equal(12, clustered[7].length)
			assert_equal(4, clustered[8].length)
		end

		def test_aggregator_should_aggregate_table_by_hour
			if HAVE_LOCAL_DB
				ag = Aggregator.new
				ag.database = { :host => TESTDBHOST, :user => TESTDBUSER, :password => TESTDBPASS, :database => TESTDBDATABASE }
				queries = ag.aggregate('ifInOctets_252', 42, 2.month.ago, 1.month.ago, 1.hour)
				assert(queries.length > 2)
				assert(queries[0] =~ /DELETE FROM ifInOctets_252 WHERE id = 42 AND/)
				assert(queries[1] =~ /INSERT INTO ifInOctets_252 \(id, dtime, counter, rate\) VALUES \(/)
			end
		end

		def test_aggregator_should_run
			if HAVE_LOCAL_DB
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
		end
	end
end
