select
	(d.Date + interval h.Hours HOUR) as DateTime,
	s.site_id as `Site ID`,
	(case p.product_category_type
		when 'FLIGHTS' then 'Flights'
		when 'HOTELS' then 'Hotels'
	end) as `Product Category Type`,
	m.Site,
	m.Slice,
	m.`Start Percentage`,
	m.`End Percentage`,
	m.`Start Date`,
	m.`Start Date Formatted`,
	m.`End Date`,
	m.`End Date Formatted`
from
(select 
	distinct(date_in_et) as Date 
from historical_budgets
where date_in_et >= '2013-10-25') d,
(select 0 as Hours
	union select 1 as Hours
	union select 2 as Hours
	union select 3 as Hours
	union select 4 as Hours
	union select 5 as Hours
	union select 6 as Hours
	union select 7 as Hours
	union select 8 as Hours
	union select 9 as Hours
	union select 10 as Hours
	union select 11 as Hours
	union select 12 as Hours
	union select 13 as Hours
	union select 14 as Hours
	union select 15 as Hours
	union select 16 as Hours
	union select 17 as Hours
	union select 18 as Hours
	union select 19 as Hours
	union select 20 as Hours
	union select 21 as Hours
	union select 22 as Hours
	union select 23 as Hours) h,
(select
	distinct(site_id) as site_id
from model_slices) s,
(select
	distinct(product_category_type) as product_category_type
from model_slices) p,
(select
	s.name as Site,
	s.id as site_id,
	product_category_type,
	concat(treatment," (",model_slices.id,")") as Slice, 
	percent_from/100 as `Start Percentage`,
	percent_to/100 as `End Percentage`, 
	model_slices.created_at as `Start Date`,
	date_format(model_slices.created_at, '%Y-%m-%d %H:00:00') as `Start Date Formatted`,

	IF(deleted_at is null, CURRENT_TIMESTAMP(), deleted_at) as `End Date`,
	date_format(IF(deleted_at is null, CURRENT_TIMESTAMP(), deleted_at - interval 1 hour), '%Y-%m-%d %H:00:00') as `End Date Formatted`
from model_slices
left join sites s on s.id = model_slices.site_id) m
where m.`Start Date Formatted` <= (d.Date + interval h.Hours HOUR)
and m.`End Date Formatted` >= (d.Date + interval h.Hours HOUR)
and m.site_id = s.site_id
and m.product_category_type = p.product_category_type
