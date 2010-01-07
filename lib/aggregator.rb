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
	def minutes ; minute ; end
	def hours ; hour ; end
	def days ; day ; end
	def weeks ; week ; end
	def months ; month ; end
	def years ; year ; end
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
	# Whether to print short informational messages to stdout during processing.
	#  verbose = false
	attr_accessor :verbose

	def self.aggregate
		a = Aggregator.new
		yield(a)
		a.run
	end

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
	def rules # :nodoc:
		# Rules should always be returned sorted with oldest first.
		@rules.sort! { |a, b| b[:age] <=> a[:age] }
	end

	# Start the processing.
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

			deletes = 0
			inserts = 0
			delete_qs = 0
			insert_qs = 0
			prev_rule_end_time = nil
			table_rules.each do |rule|
				# Do drops and reduces, and collect SQL statements
				end_time = rule[:age].ago
				if rule.key? :drop
					if @dry_run
						query = drop(table, end_time)
						puts query if @verbose
					else
						drop(table, end_time) do |q|
							connection.query(q)
							deletes += connection.affected_rows
							delete_qs += 1
						end
					end
				elsif rule.key? :reduce
					interval = rule[:reduce]
					start_time = prev_rule_end_time || 0
					ids(table).each do |id|
						if @dry_run
							# Aggregate and get queries for printing
							queries = aggregate(table, id, start_time, end_time, interval)
							queries.each { |q| puts q } if @verbose
						else
							# Aggregate with immediate execution
							aggregate(table, id, start_time, end_time, interval) do |q|
								connection.query(q)
								if q =~ /INSERT/
									inserts += connection.affected_rows
									insert_qs += 1
								elsif q =~ /DELETE/
									deletes += connection.affected_rows
									delete_qs += 1
								end
							end
						end
					end
				end
				prev_rule_end_time = end_time

			end

			puts "  Inserted #{inserts} rows (individually)." if @verbose
			puts "  Deleted #{deletes} rows in #{delete_qs} queries (about #{(deletes / delete_qs).to_i} rows/q)" if @verbose

			if @optimize
				optimize_table(table)
			end

			set_pruned(table)

			puts "  #{to_process.length} tables left to check." if @verbose
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

	# Create a row that summarizes all those passed in.
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

	# Guess whether a certain dataset seems to be gauge data or not.
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
				if defined? yield
					yield delete
					yield insert
				else
					queries << delete << insert
				end
			end
		end
		return queries
	end

	# Create SQL command to delete old data from a table.
	def drop(table, end_time)
		q = "DELETE FROM #{table} WHERE dtime <= FROM_UNIXTIME(#{end_time})"
		if defined? yield
			yield(q)
		else
			return q
		end
	end

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

