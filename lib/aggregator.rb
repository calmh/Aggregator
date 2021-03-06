require 'extensions'
require 'mysql'
require 'date'

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
  # Table names that should always be considered gauge, regardless of autodetection.
  attr_accessor :always_gauge

  # Convenience method to run aggregation. Passes an aggregator instance to the block for configuration, and the runs aggregation.
  def self.aggregate
    aggregator = Aggregator.new
    yield(aggregator)
    aggregator.run
  end

  def initialize
    @rules = []
    @index = true
    @optimize = true
    @excludes = [ 'interfaces', 'routers', 'pruned' ]
    @runlimit = nil
    @reaggregate_interval = 1.month
    @database = nil
    @dry_run = false
    @verbose = false
    @always_gauge = []
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
      if needs_pruning?(table)
        @deletes = 0
        @inserts = 0
        @delete_qs = 0
        @insert_qs = 0
        verbose "Looking at #{table}"

        table_rules = rules_for(table)
        create_indexes(table)

        prev_rule_end_time = nil
        table_rules.each do |rule|
          end_time = rule[:age].ago
          interval = rule[:reduce]
          start_time = prev_rule_end_time || 0

          if rule.key? :drop
            # Drop data older than end_time
            drop_older_db(table, end_time)
          elsif rule.key? :reduce
            # Reduce data older than start_time to a lower precision (aggregate).
            # Aggregate separately for each ID in the table
            ids(table).each do |id|
              aggregate_db(table, id, start_time, end_time, interval)
            end
          end
          prev_rule_end_time = end_time
        end

        if @inserts > 0
          verbose "  Inserted #{@inserts} rows (individually)."
        end

        if @deletes > 0
          verbose "  Deleted #{@deletes} rows in #{@delete_qs} queries (about #{(@deletes / @delete_qs).to_i} rows/q)"
        end

        optimize_table(table)
        set_pruned(table)
      end
    end
  end

  private

  #
  # Logic stuff
  #

  def configured_as_gauge?(table_name)
    @always_gauge.each do |expression|
      return true if expression.kind_of?(String) && table_name == expression
      return true if expression.kind_of?(Regexp) && table_name =~ expression
    end
    return false
  end

  def cluster_rows(rows, interval)
    @clusters = []
    @cur_cluster = []
    @interval_end = (rows[0][0] / interval + 1).to_i * interval
    rows.each do |row|
      add_row_to_clusters(row, interval)
    end
    @clusters << @cur_cluster if @cur_cluster.length > 0
    return @clusters
  end

  def add_row_to_clusters(row, interval)
    if row[0] > @interval_end
      @clusters << @cur_cluster if @cur_cluster.length > 0
      @cur_cluster = []
      @interval_end += interval
    end
    @cur_cluster << row
  end

  # Check if the named table should be excluded.
  def exclude?(table)
    @excludes.each do |excl|
      return true if table.apprmatch(excl)
    end
    return false
  end

  # Return a list of rules that applies to the specified table.
  def rules_for(table)
    by_string, ages = rules_by_string(table, [])
    by_regexp, ages = rules_by_regexp(table, ages)
    for_all, ages = rules_for_all(table, ages)

    valid_rules = by_string + by_regexp + for_all
    valid_rules.sort { |a, b| b[:age] <=> a[:age] }
  end

  def rules_by_string(table, ages)
    rules = @rules.select { |rule| rule[:table].kind_of?(String) && table == rule[:table] }
    ages = rules.map { |rule| rule[:age] }
    return [ rules, ages ]
  end

  def rules_by_regexp(table, ages)
    rules = @rules.select { |rule| rule[:table].kind_of?(Regexp) && table =~ rule[:table] && !ages.include?(rule[:age]) }
    ages += rules.map { |rule| rule[:age] }
    return [ rules, ages ]
  end

  def rules_for_all(table, ages)
    rules = @rules.select { |rule| rule[:table] == :all && !ages.include?(rule[:age]) }
    ages += rules.map { |rule| rule[:age] }
    return [ rules, ages ]
  end

  # Create a row that summarizes all those passed in.
  def summary_row(rows, table_name=nil)
    rlen = rows.length
    return nil if rows.nil? || rlen == 0
    return rows[0] if rlen == 1

    return sum_prechecked_rows(rows, table_name)
  end

  def sum_prechecked_rows(rows, table_name)
    rlen = rows.length
    last_row = rows[rlen - 1]
    average = rows.inject(0) { |sum, row| sum += row[2] } / rlen

    if configured_as_gauge?(table_name) || gauge?(rows)
      return [ last_row[0], average, average ]
    else
      counter_sum = rows.inject(0) { |sum, row| sum += row[1] }
      return [ last_row[0], counter_sum, average ]
    end
  end

  # Guess whether a certain dataset seems to be gauge data or not.
  def gauge?(rows)
    rlen = rows.length
    required_gauge_confidence = 2
    probably_gauge = 0
    tested = []
    # Pick a start row, any row.
    index = rlen - 1
    # Do a few random tests until we are fairly sure whether this is a gauge or normal counter.
    while tested.length < rlen && probably_gauge >= 0 && probably_gauge < required_gauge_confidence
      index = rand(rlen) while tested.include? index
      row = rows[index]
      tested << index
      probably_gauge += 1 if row[1] == row[2]
    end
    return probably_gauge >= required_gauge_confidence
  end

  #
  # Pruning stuff
  #

  # Verify that we have a pruned table, or create it if not.
  def create_pruned_table(conn)
    if !conn.list_tables.include?('pruned')
      query = "CREATE TABLE `pruned` ( `table_name` VARCHAR(64) NOT NULL PRIMARY KEY, `prune_time` DATETIME NOT NULL )"
      if @dry_run
        verbose query
      else
        conn.query query
      end
    end
  end

  # Get the latest prune time for specified table, or nil if never
  def get_pruned(table)
    query = "SELECT prune_time FROM pruned WHERE table_name = '#{table}'"
    if @dry_run
      verbose query
    else
      res = connection.query query
      if res.num_rows == 1
        row = res.fetch_row
        return DateTime.parse(row[0])
      end
    end
    return nil
  end

  # Mark the specified table as pruned
  def set_pruned(table)
    if !@dry_run
      conn = connection
      res = conn.query("UPDATE pruned SET prune_time = now() WHERE table_name = '#{table}'")
      if conn.affected_rows == 0
        conn.query("INSERT INTO pruned (table_name, prune_time) VALUES ('#{table}', now())")
      end
      verbose "  Updated prune_time."
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
    @queries = []
    clusters = cluster_rows(rows, interval)
    clusters.each do |cluster|
      aggregate_cluster(cluster, table, id)
    end
    return @queries
  end

  def aggregate_cluster(cluster, table, id)
    return if cluster.length <= 1
    summary = summary_row(cluster, table)
    insert = "INSERT INTO #{table} (id, dtime, counter, rate) VALUES (#{id}, FROM_UNIXTIME(#{summary[0]}), #{summary[1]}, #{summary[2]})"
     delete = "DELETE FROM #{table} WHERE id = #{id} AND dtime IN (" + cluster.collect{ |row| "FROM_UNIXTIME(#{row[0]})" }.join(", ") + ")"
     @queries << delete << insert
  end

  def aggregate_db(table, id, start_time, end_time, interval)
    queries = aggregate(table, id, start_time, end_time, interval)
    if !@dry_run
      conn = connection
      conn.query "SET AUTOCOMMIT=0"
      queries.each do |q|
        conn.query(q)
        if q =~ /INSERT/
          @inserts += conn.affected_rows
          @insert_qs += 1
        elsif q =~ /DELETE/
          @deletes += conn.affected_rows
          @delete_qs += 1
        end
      end
      conn.query "COMMIT"
      conn.query "SET AUTOCOMMIT=1"
    else
      verbose queries
    end
  end


  # Create SQL command to delete old data from a table.
  def drop_older(table, end_time)
    query = "DELETE FROM #{table} WHERE dtime <= FROM_UNIXTIME(#{end_time})"
    return query
  end

  # Drop data older than +end_time+ from +table+.
  def drop_older_db(table, end_time)
    query = drop_older(table, end_time)
    if !@dry_run
      conn = connection
      conn.query query
      @deletes += conn.affected_rows
      @delete_qs += 1
    else
      verbose query
    end
  end


  # Get a database connection or raise an error if we can't
  def connection
    return nil if @dry_run
    raise Mysql::Error, "Cannot connect without database information" if @database.nil?
    if !@conn
      @conn = Mysql::new(@database[:host], @database[:user], @database[:password], @database[:database])
      create_pruned_table(@conn)
    end
    @conn
  end

  # Return a list of all non-excluded tables
  def tables
    connection.list_tables.select { |table| !exclude?(table) }
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
    res.each { |row| rows << [ row[0].to_i, row[1].to_i, row[2].to_i ] }
    return rows
  end

  # Create necessary indexes, ignoring exceptions if they already exist.
  def create_indexes(table)
    return if !@index
    if !@dry_run
      begin
        connection.query("ALTER IGNORE TABLE #{table} ADD PRIMARY KEY (dtime, id)")
        verbose "  Created primary key index."
      rescue
        nil # If we couldn't create the index (because it exists), that's OK.
      end
    end
  end

  # Optimize table
  def optimize_table(table)
    return if !@optimize
    if !@dry_run
      connection.query("OPTIMIZE TABLE #{table}")
      verbose "  Optimized table."
    end
  end

  # Conditionally print verbose data
  def verbose(*args)
    if @verbose
      $stderr.puts args
    end
  end

  # Check if this table needs pruning, that is if it hasn't been pruned during the last +reaggregate_interval+.
  def needs_pruning?(table)
    last_pruned = get_pruned(table)
    return false if !last_pruned.nil? && DateTime.now < last_pruned + reaggregate_interval / 86400.0

    table_rules = rules_for(table)
    return false if table_rules.length == 0

    true
  end
end

