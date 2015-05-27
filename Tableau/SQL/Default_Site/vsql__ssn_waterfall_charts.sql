select
	date_in_et as Date,
	(case when site_type is null then 'Total' else
	(case site_type 
		when 'CHEAPTICKETS' then 'CheapTickets'
		when 'EBOOKERS' then 'eBookers'
		when 'ORBITZ_GLOBAL' then 'Orbitz'
		when 'TRAVELOCITY' then 'Travelocity'
		when 'HOTELCLUB' then 'HotelClub'
		else site_type
	end) end) as Site,
	(case time_frame 
		when 'DAY' then 'Day'
		when 'PAST_7_DAYS' then 'Past 7 Days'
		when 'PAST_28_DAYS' then 'Past 28 Days'
		when 'QUARTER_TO_DATE' then 'Quarter to Date'
		when 'YEAR_TO_DATE' then 'Year to Date'
		else time_frame
	end) as "Time Frame",
	(case metric 
		when 'ACTIVE_HOTELS' then 'Active Hotels'
		when 'AD_CTR' then 'Ad CTR'
		when 'ADS_PER_PAGE' then 'Ads per Page'
		when 'AVAILABLE_PAGE_ECPM' then 'Available Page eCPM'
		when 'BUDGET_PER_ACTIVE_HOTEL' then 'Budget per Active Hotel'
		when 'CLICKS' then 'Clicks'
		when 'CPC' then 'CPC'
		when 'GROSS_REVENUE' then 'Gross Revenue'
		when 'IMPRESSIONS' then 'Impressions'
		when 'PAGE_CTR' then 'Page CTR'
		when 'PAGE_FILL_RATE' then 'Page Fill Rate'
		when 'PAGES_AVAILABLE' then 'Pages Available'
		when 'PAGES_SERVED' then 'Pages Served'
		when 'SERVED_PAGE_ECPM' then 'Served Page eCPM'
		when 'SOLD_HOTELS' then 'Sold Hotels'
		else metric
	end) as Metric,
	current as Current,
	previous as Previous,
	previous_year as "Previous Year"
from intent_media_production.ssn_site_statistics_by_period
