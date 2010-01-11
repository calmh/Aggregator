class Class
	# Make all private methods public and return a list of them
	def publicize_methods
		saved_private_instance_methods = self.private_instance_methods
		self.class_eval { public *saved_private_instance_methods }
		return saved_private_instance_methods
	end

	# Make the specified methods private
	def privatize_methods(methods)
		self.class_eval { private *methods }
	end
end

def conn
	$conn ||= Mysql::new(TESTDBHOST, TESTDBUSER, TESTDBPASS, nil)
end

TESTDBHOST = 'localhost'
TESTDBUSER = 'root'
TESTDBPASS = nil
TESTDBDATABASE = 'rtg'

# Generate a few counter rows
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

# Generate a few gauge rows
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

def db_teardown
	conn.query("DROP DATABASE #{TESTDBDATABASE}")
end

def db_setup
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


