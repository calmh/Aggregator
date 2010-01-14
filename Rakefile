task :default => [ 'test:default', :rdoc ]

namespace :test do
	desc "Run unit tests"
	task :default do
		ruby "-Ilib test/aggregator_test.rb"
	end

	desc "Run unit tests, with local database"
	task :with_db do
		ENV["HAVE_LOCAL_DB"] = "true"
		ruby "-Ilib test/aggregator_test.rb"
	end

	desc "Run unit tests, with local database, and generate coverage"
	task :rcov do
		ENV["HAVE_LOCAL_DB"] = "true"
		sh "rcov -Ilib test/aggregator_test.rb"
	end
end

desc "Create rdoc documentation"
task :rdoc do
	sh "rdoc lib"
end
