# just run this script: 'ruby example.rb'

require 'nokogiri'

@data_repo_location = '../..'
@directories = ['Tableau/Workbooks/Default_Site', 'Tableau/Workbooks/UnderlyingData_Site']

def global_queries_hash(directory)
	global_queries_hash = {}
	
	files = all_files(directory)
	
	files.each do |file_name|
		doc = Nokogiri::XML(File.open("#{@data_repo_location}/#{directory}/#{file_name}"))
		global_queries_hash[file_name] = local_queries(doc)
	end

	return global_queries_hash
end

def local_queries(doc)
	local_queries = ''
	
	relations = doc.xpath('//connection//relation')
 	
 	relations.each do |relation|
 		local_queries << relation.text() if relation['type'] == 'text'
 	end
	
	return local_queries
end

def all_files(directory)
	Dir.entries("#{@data_repo_location}/#{directory}")[2..-1]
end

def print_where_used(test_keyword)
	@directories.each do |directory|
		global_queries_hash(directory).each do |k,v|
			puts k if v.include?(test_keyword)
		end
	end
end

def run
	puts 'Enter a table name or column name (or whatever you might want to find inside a bunch of SQL):'
	test_keyword = gets.chomp
	puts 'This is used in the following Tableau reports:'
	print_where_used(test_keyword)
end

run
