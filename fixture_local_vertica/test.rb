require 'minitest/autorun'
require 'time'
require_relative 'vertica_fixturer'

class TestVerticaFixturer < Minitest::Test
	def setup
		@v = VerticaFixturer.new
	end
	
	def teardown
		@v.close
	end
	
	def test_inserting_row_no_options
		@v.truncate_tables(['ad_calls'])
		@v.insert_row('ad_calls')
		
		rows_in_ad_calls = @v.query('SELECT COUNT(1) FROM intent_media_log_data_development.ad_calls')
		requested_at_in_et = extract('intent_media_log_data_development.ad_calls', 'requested_at_in_et')
		ad_unit_id = extract('intent_media_log_data_development.ad_calls', 'ad_unit_id')
		
		assert_equal(1, rows_in_ad_calls.length)
		refute_nil(requested_at_in_et)
		assert_nil(ad_unit_id)
	end
	
	def test_inserting_row_with_options
		@v.truncate_tables(['ad_calls'])
		@v.insert_row('ad_calls', :ad_unit_id => '2')
		
		ad_unit_id = extract('intent_media_log_data_development.ad_calls', 'ad_unit_id')
		show_ads = extract('intent_media_log_data_development.ad_calls', 'show_ads')
		
		refute_nil(ad_unit_id)
		assert_nil(show_ads)
	end
	
	def test_inserting_row_non_default_schema
		@v.truncate_tables(['sites'], 'intent_media_development')
		@v.insert_row('sites', :schema => 'intent_media_development', :privacy_policy => 'a')
		
		privacy_policy = extract('intent_media_development.sites', 'privacy_policy')
		advertiser_id = extract('intent_media_development.sites', 'advertiser_id')
		
		refute_nil(privacy_policy)
		assert_nil(advertiser_id)
	end
	
	def test_inserting_row_with_table_specific_default
		@v.truncate_tables(['clicks'])
		@v.insert_row('clicks')
		
		ip_address_blacklisted = extract('intent_media_log_data_development.clicks', 'ip_address_blacklisted')
		
		assert_equal(false, ip_address_blacklisted)
	end
	
	private
	
	def extract(table_name, column_name)
		@v.query("SELECT * FROM #{table_name}").first[column_name.to_sym]
	end
	
end
