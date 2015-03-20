require 'vertica'
require 'highline/import'

@connection = Vertica.connect({
  :user => 'dbadmin',
  :host => 'localhost'
})

def columns_to_insert(options)
	keys = options.keys
	keys.push('requested_at_in_et') unless keys.include?(:requested_at_in_et)
	return keys.join(',')
end

def values_to_add(options)
	default_date = options[:requested_at_in_et] ? options[:requested_at_in_et] : (Time.now - 3).to_s[0..-7] #getting rid of zone
	values = options.values
	
	values.push(default_date) unless options.keys.include?(:requested_at_in_et)
	return "'" + values.join("','") + "'"
end

def truncate_tables(database, table_names)
	table_names.each do |table_name|
		@connection.query("TRUNCATE TABLE #{database}.#{table_name}; COMMIT;")
	end
end

def insert_row(database, table_name, options = {})
	query_string = "INSERT INTO #{database}.#{table_name} (#{columns_to_insert(options)}) VALUES (#{values_to_add(options)})"
	@connection.query(query_string + '; COMMIT;')
end

### begin example workflow

truncate_tables('intent_media_log_data_development', ['ad_calls', 'clicks'])

insert_row('intent_media_log_data_development', 'ad_calls', :request_id => 'a', :product_category_type => 'FLIGHTS',
:site_country => 'US', :ip_country_code => 'GB', :outcome_type => 'SERVED',
:ad_unit_type => 'CT', :site_id => '1')

insert_row('intent_media_log_data_development', 'clicks', :ad_call_request_id => 'a', :request_id => 'b',
:requested_at_date_in_et => '2015-03-01', :ip_address_blacklisted => '0',
:fraudulent => '0')

# insert_row('intent_media_log_data_development', 'impressions', :ad_unit_id => "2", :requested_at_in_et => '2015-03-01 12:12:12', :device_family => "MOBILE")
# insert_row('intent_media_log_data_development', 'filtered_advertisements', :brand_id => "5", :ad_copy_includes_price => "false")

# then you can run your SQL of choice, in code or in DBVisualizer and such.

### end example workflow

@connection.close