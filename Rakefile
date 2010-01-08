task :default => [ :test, :rdoc ]

desc "Run unit tests"
task :test do
	ruby "-Ilib test/test_aggregator.rb"
end

desc "Create rdoc documentation"
task :rdoc do
	system "rdoc lib"
end

