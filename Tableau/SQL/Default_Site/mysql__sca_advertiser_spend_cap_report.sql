select
	sc.name as "IO",
	sc.cappable_type as "Spend Cap Type",
	c.name as "Campaign Name",
	concat(ifnull(e.id, ''), ifnull(ec.id,'')) as "Advertiser ID",
	concat(ifnull(e.name,''), ifnull(ec.name,'')) as "Advertiser",
	date(convert_tz(sc.start_date, 'UTC', 'America/New_York')) as `Start Date`,
	date(convert_tz(sc.end_date,   'UTC', 'America/New_York')) as `End Date`,
	scsa.amount as `Total Spend`,
	sc.amount as `Total Budget`,
	(datediff(date(convert_tz(CURRENT_DATE(), 'UTC', 'America/New_York')), date(convert_tz(sc.start_date, 'UTC', 'America/New_York'))) + 1) as `Days So Far`,
	((datediff(date(convert_tz(CURRENT_DATE(), 'UTC', 'America/New_York')), date(convert_tz(sc.start_date, 'UTC', 'America/New_York'))) + 1) / 
	 (datediff(date(convert_tz(   sc.end_date, 'UTC', 'America/New_York')), date(convert_tz(sc.start_date, 'UTC', 'America/New_York'))) + 1)) as `Percent Time`,
	(scsa.amount / sc.amount) as `Percent Spent`,
	if(convert_tz(CURRENT_DATE(),'UTC','America/New_York') >= convert_tz(sc.start_date,'UTC','America/New_York')
		and convert_tz(CURRENT_DATE(),'UTC','America/New_York') <= convert_tz(sc.end_date,'UTC','America/New_York'), 'Currently Active', 'Expired') as Status	
from spending_caps sc
left join entities e on e.id = sc.cappable_id and sc.cappable_type = 'Entity'
left join campaigns c on c.id = sc.cappable_id and sc.cappable_type = 'Campaign'
left join entities ec on ec.id = c.advertiser_id
left join spending_cap_spent_amounts scsa on sc.id = scsa.spending_cap_id
where ifnull(e.active,ec.active) = 1