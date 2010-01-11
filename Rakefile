task :default => [ :test, :rdoc ]

desc "Run unit tests"
task "test" do
	ruby "-Ilib test/aggregator_test.rb"
end

desc "Run unit tests, with local database"
task "test:db" do
	ENV["HAVE_LOCAL_DB"] = "true"
	ruby "-Ilib test/aggregator_test.rb"
end

desc "Create rdoc documentation"
task "rdoc" do
	system "rdoc lib"
end

