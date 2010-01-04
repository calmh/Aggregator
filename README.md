RTG Aggregator
==============

One of the prominent features of RTG is that there is no averaging involved
after the actual poll. This can be advantageous, but if you poll data for many
devices at a short interval you soon end up with hundreds of gigabytes of data.
RTG Aggregator (hereafter "aggregator") is a solution to that problem.

With aggregator you specify a rule set for how you want old data to be managed.
This can be a stepwise degradation of precision over time, with the possibility
of dropping data older than a certain threshold. Configuration is done flexibly
in Ruby. An example aggregator script, complete and working, looks like this:

    require 'aggregator'
    Aggregator.aggregate do |a|
    	a.database = { :host => ..., :user => ..., :password => ..., :database => ... }
    	a.rules << { :table => /^if/, :age => 1.month, :reduce => 15.minute }
    	a.rules << { :table => /^if/, :age => 6.month, :reduce => 1.hour }
    	a.rules << { :table => /^if/, :age => 2.year,  :reduce => 8.hour }
    	a.rules << { :table => /^adsl|^dlink/, :age => 1.month, :reduce => 1.hour }
    	a.rules << { :table => /^adsl|^dlink/, :age => 1.year,  :reduce => 1.day }
    	a.rules << { :table => :all, :age => 3.year,  :drop => true }
    	a.runlimit = 50.minute
    	a.verbose = true
    end

Going through this, line by line, we see:

    require 'aggregator'

Load the aggregator library.

    Aggregator.do |a|

Start aggregation with "a" as the object holding the configuration.

    a.database = { :host => ..., :user => ..., :password => ..., :database => ... }

Set the database access parameters. This needs to be a MySQL database following
the "traditional" RTG schema.

    a.rules << { :table => /^if/, :age => 1.month, :reduce => 15.minute }
    a.rules << { :table => /^if/, :age => 6.month, :reduce => 1.hour }
    a.rules << { :table => /^if/, :age => 2.year,  :reduce => 8.hour }

Create rules for tables matching the ^if regexp (ifInOctets and friends)
stating that data should be reduced, so that data older than two years is in
eight hour averages, data between six months and years old should be reduced to
hourly averages, and finally data between one and six months old should be kept
in 15 minute averages.

    a.rules << { :table => /^adsl|^dlink/, :age => 1.month, :reduce => 1.hour }
    a.rules << { :table => /^adsl|^dlink/, :age => 1.year,  :reduce => 1.day }

Create rules for tables matching ^adsl or ^dlink that reduces data in the same
fashion as above.

    a.rules << { :table => :all, :age => 3.year,  :drop => true }

Create a rule that matches all tabled and deletes data older than three years.

a.runlimit = 50.minute

Set a limit that the script will run for no more than 50 minutes (roughly).
This is useful if it is being run from cron, for example.

    a.verbose = true

Set the verbose flag to get some output describing what it's doing.

    end

End of the configuration, start processing.

For more documentation, see the generated RDoc at http://rdoc.info/projects/calmh/Aggregator

Jakob Borg
2009-12-29
