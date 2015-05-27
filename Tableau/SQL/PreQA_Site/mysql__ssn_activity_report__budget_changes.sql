select
	advertiser_id as "Advertiser ID",
	count(1) as "Budget Changes"
from hotel_ssr_advertiser_changes
where change_type = 'Budget Changed' 
and convert_tz(created_at,'UTC','America/New_York') < convert_tz(CURRENT_DATE(),'UTC','America/New_York')
group by advertiser_id