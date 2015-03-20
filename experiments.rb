require 'vertica'
require 'highline/import'

@connection = Vertica.connect({
  :user => 'dbadmin',
  :host => 'localhost'
})

@default_schema='intent_media_log_data_development'

def columns_to_insert(options)
	keys = options.keys
	
	if keys.include?(:schema)
		keys.delete(:schema)
	else
		unless keys.include?(:requested_at_in_et)
			keys.push('requested_at_in_et') 
		end
	end
	
	return keys.join(',')
end

def values_to_add(options)
	default_date = options[:requested_at_in_et] ? options[:requested_at_in_et] : (Time.now - 3).to_s[0..-7] #getting rid of zone
	values = options.values
	
	if options.keys.include?(:schema)
		values.delete(options[:schema])
	else
		unless options.keys.include?(:requested_at_in_et)
			values.push(default_date)
		end
	end
	
	return "'" + values.join("','") + "'"
end

def truncate_tables(table_names, schema=@default_schema)
	table_names.each do |table_name|
		@connection.query("TRUNCATE TABLE #{schema}.#{table_name}; COMMIT;")
	end
end

def insert_row(table_name, options = {})
	if options[:schema]
		schema = options[:schema]
	else
		schema = @default_schema
	end
	
	query_string = "INSERT INTO #{schema}.#{table_name} (#{columns_to_insert(options)}) VALUES (#{values_to_add(options)})"
	@connection.query(query_string + '; COMMIT;')
end

### begin example workflow

truncate_tables(['ad_calls', 'clicks'])
truncate_tables(['sites'], 'intent_media_development')
truncate_tables(['GC_GEO_TOTAL_CLICKS'], 'intent_media_sandbox_development')

insert_row('ad_calls', :request_id => 'a', :product_category_type => 'FLIGHTS',
:site_country => 'US', :ip_country_code => 'GB', :outcome_type => 'SERVED',
:ad_unit_type => 'CT', :site_id => '1')

insert_row('clicks', :ad_call_request_id => 'a', :request_id => 'b',
:requested_at_date_in_et => '2015-03-01', :ip_address_blacklisted => '0',
:fraudulent => '0')

insert_row('sites', :schema => 'intent_media_development', :display_name => 'EXPEDIA')

insert_row('GC_GEO_TOTAL_CLICKS', :schema => 'intent_media_sandbox_development',
:display_name => 'ORBITZ')

# then you can run your SQL of choice, in code or in DBVisualizer and such.

### end example workflow

@connection.close