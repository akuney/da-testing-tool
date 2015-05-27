require 'vertica'
require 'highline/import'

class VerticaFixturer
	attr_accessor :connection, :general_defaults
	
	def initialize
		@connection = Vertica.connect({
		  :user => 'dbadmin',
		  :host => 'localhost'
		})
		@general_defaults = {:schema => 'intent_media_log_data_development', :requested_at_in_et => (Time.now - 200000).to_s[0..-7]}
		
		@click_defaults = {:ip_address_blacklisted => 'false'}
		@ad_call_defaults = {:ip_address_blacklisted => 'false'}
		@conversion_defaults = {:ip_address_blacklisted => 'false'}
		@table_specific_defaults = {:clicks => @click_defaults, :ad_calls => @ad_call_defaults, :conversions => @conversion_defaults}
	end
	
	def close
		@connection.close
	end
	
	def query(sql)
		@connection.query(sql + '; COMMIT;')
	end

	def truncate_tables(table_names, schema=@general_defaults[:schema])
		table_names.each do |table_name|
			@connection.query("TRUNCATE TABLE #{schema}.#{table_name}; COMMIT;")
		end
	end

	def insert_row(table_name, options = {})
		if options[:schema]
			schema = options[:schema]
		else
			schema = @general_defaults[:schema]
		end
		
		options.merge!(get_defaults(table_name))
		
		query_string = "INSERT INTO #{schema}.#{table_name} (#{columns_to_insert(options)}) VALUES (#{values_to_add(options)})"
		self.query(query_string)
	end
	
	
	private 
	
	
	def columns_to_insert(options)
		keys = options.keys
	
		if keys.include?(:schema)
			keys.delete(:schema)
		elsif !keys.include?(:requested_at_in_et)
			keys.push('requested_at_in_et') 
		end
	
		return keys.join(',')
	end

	def values_to_add(options)
		default_date = options[:requested_at_in_et] ? options[:requested_at_in_et] : @general_defaults[:requested_at_in_et]
		values = options.values
		keys = options.keys
	
		if keys.include?(:schema)
			values.delete(options[:schema])
		elsif !keys.include?(:requested_at_in_et)
			values.push(default_date)
		end
	
		return "'" + values.join("','") + "'"
	end
	
	def get_defaults(table_name)
		@table_specific_defaults[table_name.to_sym] || {}
	end
end
