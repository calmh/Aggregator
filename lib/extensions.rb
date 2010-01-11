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

class String
	# Match string or regexp
	def apprmatch(obj)
		obj.kind_of?(String) && self == obj || obj.kind_of?(Regexp) && self =~ obj
	end
end

