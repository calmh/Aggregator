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

