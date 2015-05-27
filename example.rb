require_relative 'vertica_fixturer'

def run_stuff(&blk)
	@v = VerticaFixturer.new
	yield
	@v.close
end

run_stuff do
	#put your workflow here
	
	@v.truncate_tables(['entities'], 'intent_media_development')
	@v.truncate_tables(['sites'], 'intent_media_development')
	@v.truncate_tables(['ad_calls', 'conversions'])
	
	@v.insert_row('entities', :schema => 'intent_media_development', :id => '106')
	@v.insert_row('sites', :schema => 'intent_media_development', :id => '29')
	
	@v.insert_row('ad_calls', :publisher_user_id => 'a', :publisher_id => '106', :site_id => '29',
	:requested_at_date_in_et => '2015-02-02', :ad_unit_type => 'CT')
	@v.insert_row('ad_calls', :publisher_user_id => 'a', :publisher_id => '106', :site_id => '29',
	:requested_at_date_in_et => '2015-02-02', :ad_unit_type => 'CT')
	@v.insert_row('ad_calls', :publisher_user_id => 'b', :publisher_id => '106', :site_id => '29',
	:requested_at_date_in_et => '2015-02-02', :ad_unit_type => 'CT')
	@v.insert_row('ad_calls', :publisher_user_id => 'b', :publisher_id => '106', :site_id => '29',
	:requested_at_date_in_et => '2015-02-02', :ad_unit_type => 'CT')
	@v.insert_row('ad_calls', :publisher_user_id => 'c', :publisher_id => '106', :site_id => '29',
	:requested_at_date_in_et => '2015-02-02', :ad_unit_type => 'CT')
	
	@v.insert_row('conversions', :publisher_user_id => 'a', :net_conversion_value => '50',
	:order_id => 'a', :product_category_type => 'HOTELS',
	:site_id => '29', :entity_id => '106', :requested_at_date_in_et => '2015-02-02')
	@v.insert_row('conversions', :publisher_user_id => 'b', :net_conversion_value => '60',
	:order_id => 'b', :product_category_type => 'FLIGHTS',
	:site_id => '29', :entity_id => '106', :requested_at_date_in_et => '2015-02-02')
	@v.insert_row('conversions', :publisher_user_id => 'b', :net_conversion_value => '190',
	:order_id => 'c', :product_category_type => 'HOTELS',
	:site_id => '29', :entity_id => '106', :requested_at_date_in_et => '2015-02-02')
end