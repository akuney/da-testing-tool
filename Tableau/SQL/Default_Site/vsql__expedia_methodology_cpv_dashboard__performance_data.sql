-- TODO: remove repetition of code, clean this up
select
	min(agg_dat3a."Product") as "Product",
	agg_dat3a."Date" as "End Date",
	min(agg_dat3a."Site") as "Pub",
	min(agg_dat3a."Pages Served") as "Pages Served",
	min(agg_dat3a."Net Media Revenue") as "Net Media Revenue",
	min(agg_dat3a."Pages Served Two Month") as "Pages Served Two Month",
	min(agg_dat3a."Net Media Revenue Two Month") as "Net Media Revenue Two Month",
	min(agg_dat3a."Pages Served Three Month") as "Pages Served Three Month",
	min(agg_dat3a."Net Media Revenue Three Month") as "Net Media Revenue Three Month",
	sum(agg_dat3b."Pages Served") as "Pages Served Four Month",
	sum(agg_dat3b."Net Media Revenue") as "Net Media Revenue Four Month"
from
(
	select
		min(agg_dat2a."Product") as "Product",
		agg_dat2a."Date" as "Date",
		min(agg_dat2a."Pub") as "Pub",
		min(agg_dat2a."Site") as "Site",
		min(agg_dat2a."Pages Served") as "Pages Served",
		min(agg_dat2a."Net Media Revenue") as "Net Media Revenue",
		min(agg_dat2a."Pages Served Two Month") as "Pages Served Two Month",
		min(agg_dat2a."Net Media Revenue Two Month") as "Net Media Revenue Two Month",
		sum(agg_dat2b."Pages Served") as "Pages Served Three Month",
		sum(agg_dat2b."Net Media Revenue") as "Net Media Revenue Three Month"
	from
	(
		select
		min(agg_dat1a."Product") as "Product",
		agg_dat1a."Date" as "Date",
		min(agg_dat1a."Pub") as "Pub",
		min(agg_dat1a."Site") as "Site",
		min(agg_dat1a."Pages Served") as "Pages Served",
		min(agg_dat1a."Net Media Revenue") as "Net Media Revenue",
		sum(agg_dat1b."Pages Served") as "Pages Served Two Month",
		sum(agg_dat1b."Net Media Revenue") as "Net Media Revenue Two Month"
		from
		(
			select
			dat."Product",
			dat."Date",
			dat."Pub",
			dat."Site",
			sum(dat."Pages Served") as "Pages Served",
			sum(dat."Net Media Revenue") as "Net Media Revenue"
			from
			(
				select 
					last_day(aggregation_level_date_in_et) as Date,
					(case 
						when e.name = 'Orbitz' then 'OWW' 
						when e.name = 'Kayak Software Corporation' then 'Kayak' 
						else e.name 
					end) as "Pub",
					s.display_name as "Site",
					'Flights' as "Product",
					sum(ad_unit_served_count) as "Pages Served",
					sum(net_revenue_sum) as "Net Media Revenue"
				from intent_media_production.air_ct_media_performance_aggregations acmpa
				left join intent_media_production.ad_units au on acmpa.ad_unit_id = au.id		
				left join intent_media_production.sites s on s.id = au.site_id 
				left join intent_media_production.entities e on e.id = s.publisher_id 
				group by 
					last_day(aggregation_level_date_in_et),
					(case 
						when e.name = 'Orbitz' then 'OWW' 
						when e.name = 'Kayak Software Corporation' then 'Kayak' 
						else e.name 
					end),
					s.display_name
					
				union
				
				select 
					last_day(aggregation_level_date_in_et) as Date,
					(case 
						when e.name = 'Orbitz' then 'OWW' 
						when e.name = 'Kayak Software Corporation' then 'Kayak' 
						else e.name 
					end) as "Pub",
					s.display_name as "Site",
					'Hotels' as "Product",
					sum(served_ad_count) as "Pages Served",
					sum(net_revenue_sum) as "Net Media Revenue"
				from intent_media_production.hotel_ct_media_performance_aggregations hcmpa
				left join intent_media_production.ad_units au on hcmpa.ad_unit_id = au.id		
				left join intent_media_production.sites s on s.id = au.site_id 
				left join intent_media_production.entities e on e.id = s.publisher_id 
				group by 
					last_day(aggregation_level_date_in_et),
					(case 
						when e.name = 'Orbitz' then 'OWW' 
						when e.name = 'Kayak Software Corporation' then 'Kayak' 
						else e.name 
					end),
					s.display_name
			) dat
			group by
				dat."Product",
				dat."Date",
				dat."Pub",
				dat."Site"
		) agg_dat1a,
		(
			select
			dat."Product",
			dat."Date",
			dat."Pub",
			dat."Site",
			sum(dat."Pages Served") as "Pages Served",
			sum(dat."Net Media Revenue") as "Net Media Revenue"
			from
			(
				select 
					last_day(aggregation_level_date_in_et) as Date,
					(case 
						when e.name = 'Orbitz' then 'OWW' 
						when e.name = 'Kayak Software Corporation' then 'Kayak' 
						else e.name 
					end) as "Pub",
					s.display_name as "Site",
					'Flights' as "Product",
					sum(ad_unit_served_count) as "Pages Served",
					sum(net_revenue_sum) as "Net Media Revenue"
				from intent_media_production.air_ct_media_performance_aggregations acmpa
				left join intent_media_production.ad_units au on acmpa.ad_unit_id = au.id		
				left join intent_media_production.sites s on s.id = au.site_id 
				left join intent_media_production.entities e on e.id = s.publisher_id 
				group by 
					last_day(aggregation_level_date_in_et),
					(case 
						when e.name = 'Orbitz' then 'OWW' 
						when e.name = 'Kayak Software Corporation' then 'Kayak' 
						else e.name 
					end),
					s.display_name
				
				union
				
				select 
					last_day(aggregation_level_date_in_et) as Date,
					(case 
						when e.name = 'Orbitz' then 'OWW' 
						when e.name = 'Kayak Software Corporation' then 'Kayak' 
						else e.name 
					end) as "Pub",
					s.display_name as "Site",
					'Hotels' as "Product",
					sum(served_ad_count) as "Pages Served",
					sum(net_revenue_sum) as "Net Media Revenue"
				from intent_media_production.hotel_ct_media_performance_aggregations hcmpa
				left join intent_media_production.ad_units au on hcmpa.ad_unit_id = au.id		
				left join intent_media_production.sites s on s.id = au.site_id 
				left join intent_media_production.entities e on e.id = s.publisher_id 
				group by 
					last_day(aggregation_level_date_in_et),
					(case 
						when e.name = 'Orbitz' then 'OWW' 
						when e.name = 'Kayak Software Corporation' then 'Kayak' 
						else e.name 
					end),
					s.display_name
			) dat
			group by
				dat."Product",
				dat."Date",
				dat."Pub",
				dat."Site"
		) agg_dat1b
	where agg_dat1a."Product" = agg_dat1b."Product"
		and agg_dat1a."Pub" = agg_dat1b."Pub"
		and agg_dat1a."Site" = agg_dat1b."Site"
		and agg_dat1a."Date" between agg_dat1b."Date" and agg_dat1b."Date" + 60
	group by agg_dat1a."Product", agg_dat1a."Date", agg_dat1a."Pub", agg_dat1a."Site"
	) agg_dat2a,
	(
		select
			dat."Product",
			dat."Date",
			dat."Pub",
			dat."Site",
			sum(dat."Pages Served") as "Pages Served",
			sum(dat."Net Media Revenue") as "Net Media Revenue"	
		from
		(
			select 
				last_day(aggregation_level_date_in_et) as Date,
				(case 
					when e.name = 'Orbitz' then 'OWW' 
					when e.name = 'Kayak Software Corporation' then 'Kayak' 
					else e.name 
				end) as "Pub",
				s.display_name as "Site",
				'Flights' as "Product",
				sum(ad_unit_served_count) as "Pages Served",
				sum(net_revenue_sum) as "Net Media Revenue"
			from intent_media_production.air_ct_media_performance_aggregations acmpa
			left join intent_media_production.ad_units au on acmpa.ad_unit_id = au.id		
			left join intent_media_production.sites s on s.id = au.site_id 
			left join intent_media_production.entities e on e.id = s.publisher_id 
			group by 
				last_day(aggregation_level_date_in_et),
				(case 
					when e.name = 'Orbitz' then 'OWW' 
					when e.name = 'Kayak Software Corporation' then 'Kayak' 
					else e.name 
				end),
				s.display_name
			
			union
			
			select 
				last_day(aggregation_level_date_in_et) as Date,
				(case 
					when e.name = 'Orbitz' then 'OWW' 
					when e.name = 'Kayak Software Corporation' then 'Kayak' 
					else e.name 
				end) as "Pub",
				s.display_name as "Site",
				'Hotels' as "Product",
				sum(served_ad_count) as "Pages Served",
				sum(net_revenue_sum) as "Net Media Revenue"
			from intent_media_production.hotel_ct_media_performance_aggregations hcmpa
			left join intent_media_production.ad_units au on hcmpa.ad_unit_id = au.id		
			left join intent_media_production.sites s on s.id = au.site_id 
			left join intent_media_production.entities e on e.id = s.publisher_id 
			group by 
				last_day(aggregation_level_date_in_et),
				(case 
					when e.name = 'Orbitz' then 'OWW' 
					when e.name = 'Kayak Software Corporation' then 'Kayak' 
					else e.name 
				end),
				s.display_name
		) as dat
		group by
			dat."Product",
			dat."Date",
			dat."Pub",
			dat."Site"
	) agg_dat2b
	where agg_dat2a."Product" = agg_dat2b."Product"
		and agg_dat2a."Pub" = agg_dat2b."Pub"
		and agg_dat2a."Site" = agg_dat2b."Site"
		and agg_dat2a."Date" between agg_dat2b."Date" and agg_dat2b."Date" + 75
	group by agg_dat2a."Product", agg_dat2a."Date", agg_dat2a."Pub", agg_dat2a."Site"
) agg_dat3a,
(
	select
		dat."Product",
		dat."Date",
		dat."Pub",
		dat."Site",
		sum(dat."Pages Served") as "Pages Served",
		sum(dat."Net Media Revenue") as "Net Media Revenue"
	from
	(
		select 
			last_day(aggregation_level_date_in_et) as Date,
			(case 
				when e.name = 'Orbitz' then 'OWW' 
				when e.name = 'Kayak Software Corporation' then 'Kayak' 
				else e.name 
			end) as "Pub",
			s.display_name as "Site",
			'Flights' as "Product",
			sum(ad_unit_served_count) as "Pages Served",
			sum(net_revenue_sum) as "Net Media Revenue"
		from intent_media_production.air_ct_media_performance_aggregations acmpa
		left join intent_media_production.ad_units au on acmpa.ad_unit_id = au.id		
		left join intent_media_production.sites s on s.id = au.site_id 
		left join intent_media_production.entities e on e.id = s.publisher_id 
		group by 
			last_day(aggregation_level_date_in_et),
			(case 
				when e.name = 'Orbitz' then 'OWW' 
				when e.name = 'Kayak Software Corporation' then 'Kayak' 
				else e.name 
			end),
			s.display_name
			
		union
		
		select 
			last_day(aggregation_level_date_in_et) as Date,
			(case 
				when e.name = 'Orbitz' then 'OWW' 
				when e.name = 'Kayak Software Corporation' then 'Kayak' 
				else e.name 
			end) as "Pub",
			s.display_name as "Site",
			'Hotels' as "Product",
			sum(served_ad_count) as "Pages Served",
			sum(net_revenue_sum) as "Net Media Revenue"
		from intent_media_production.hotel_ct_media_performance_aggregations hcmpa
		left join intent_media_production.ad_units au on hcmpa.ad_unit_id = au.id		
		left join intent_media_production.sites s on s.id = au.site_id 
		left join intent_media_production.entities e on e.id = s.publisher_id 
		group by 
			last_day(aggregation_level_date_in_et),
			(case 
				when e.name = 'Orbitz' then 'OWW' 
				when e.name = 'Kayak Software Corporation' then 'Kayak' 
				else e.name 
			end),
			s.display_name
	) as dat
	group by
		dat."Product",
		dat."Date",
		dat."Pub",
		dat."Site"
) as agg_dat3b
where agg_dat3a."Product" = agg_dat3b."Product"
	and agg_dat3a."Pub" = agg_dat3b."Pub"
	and agg_dat3a."Site" = agg_dat3b."Site"
	and agg_dat3a."Date" between agg_dat3b."Date" and agg_dat3b."Date" + 105
group by agg_dat3a."Product", agg_dat3a."Date", agg_dat3a."Pub", agg_dat3a."Site"
