task :default => [ :test, :rdoc ]

desc "Run unit tests"
task :test do
	ruby "-Ilib test/aggregator_test.rb"
end

desc "Run unit tests, with local database"
task :test_db do
	ENV["HAVE_LOCAL_DB"] = "true"
	ruby "-Ilib test/aggregator_test.rb"
end

desc "Create rdoc documentation"
task :rdoc do
	sh "rdoc lib"
end

begin
  require 'jeweler'
  Jeweler::Tasks.new do |gemspec|
    gemspec.name = "aggregator"
    gemspec.summary = "RTG Data Aggregation library"
    gemspec.description = "Aggregator is a Ruby library that does aggregation (data reduction) on old data in an RTG database."
    gemspec.email = "jakob@nym.se"
    gemspec.homepage = "http://github.com/calmh/RTG-Aggregator"
    gemspec.authors = ["Jakob Borg"]
    gemspec.add_dependency 'mysql'
  end
rescue LoadError
  puts "Jeweler not available. Install it with: gem install jeweler"
end

