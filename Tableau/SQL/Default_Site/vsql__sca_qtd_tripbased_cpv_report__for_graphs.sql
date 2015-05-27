select
	dimensions.Date,
	dimensions."Site ID",
	s.display_name as "Site",
	acuvqa.count as "Unique Visitors",
	acuva.count as "30 Day Unique Visitors",
	(case dimensions."Product Category Type" 
		when 'FLIGHTS' then 'Flights'
		when 'HOTELS' then 'Hotels'
		when 'PACKAGES' then 'Packages'
		when 'CARS' then 'Cars'
		else dimensions."Product Category Type"
	end) as "Product Category Type",
	(case dimensions."Segmentation Type"
		when 'HIGH_CONVERTING' then 'High Value'
		when 'LOW_CONVERTING' then 'Low Value'
		else dimensions."Segmentation Type"
	end) as "Segmentation Type",
	(case dimensions."Pure Group Type"
		when 'PURE' then 'Pure'
		when 'NOT_PURE' then 'Not Pure'
		else dimensions."Pure Group Type"
	end) as "Pure Group Type",
	t."Transactions",
	t."Transaction Contribution",
	t."Pages Served",
	t."Net Media Revenue"
from
	(
    select
      dates.date_in_et as Date,
      sites.site_id as "Site ID",
      products.product_category_type as "Product Category Type",
      ifnull(pure_groups.pure_group_type, 'Total') as "Pure Group Type",
      ifnull(segmentations.segmentation_type, 'Total') as "Segmentation Type"
    from
      (
        select distinct(aggregation_level_date_in_et) as date_in_et
        from intent_media_production.air_ct_transaction_performance_aggregations
      ) dates,
      (
        select 2 as site_id
        union
        select 3 as site_id
        union
        select 4 as site_id
      ) sites,
      (
        select distinct(product_category_type) as product_category_type
        from intent_media_production.air_ct_transaction_performance_aggregations
      ) products,
      (
        select distinct(pure_group_type) as pure_group_type
        from intent_media_production.air_ct_transaction_performance_aggregations
      ) pure_groups,
      (
        select distinct(segmentation_type) as segmentation_type
        from intent_media_production.air_ct_transaction_performance_aggregations
      ) segmentations
	) dimensions
left join
	(
    select
      t.*,
      media_revenue."Pages Served",
      media_revenue."Net Media Revenue"
    from
      (
        select
          t.aggregation_level_date_in_et as Date,
          t.site_id,
          t.product_category_type,
          ifnull(t.segmentation_type, 'Total') as segmentation_type,
          ifnull(t.pure_group_type, 'Total') as pure_group_type,
          sum(bookings_count * cookie_bias_adjustment_factor) as "Transactions",
          sum(net_conversion_sum * cookie_bias_adjustment_factor) as "Transaction Contribution"
        from intent_media_production.air_ct_transaction_performance_aggregations t
        group by
          t.aggregation_level_date_in_et,
          t.site_id,
          t.product_category_type,
          t.segmentation_type,
          t.pure_group_type
      ) t
    left join
      (
        select
          aggregation_level_date_in_et,
          s.id as site_id,
          sum(ad_unit_served_count) as "Pages Served",
          sum(net_revenue_sum) as "Net Media Revenue"
        from intent_media_production.air_ct_media_performance_aggregations acmpa
        left join intent_media_production.ad_units au on acmpa.ad_unit_id = au.id
        left join intent_media_production.sites s on s.id = au.site_id
        group by
          aggregation_level_date_in_et,
          s.id
      ) media_revenue
      on t.site_id = media_revenue.site_id
        and t.Date = media_revenue.aggregation_level_date_in_et
        and t.segmentation_type in ('LOW_CONVERTING', 'Total')
        and t.pure_group_type in ('NOT_PURE','Total')
        and t.product_category_type = 'FLIGHTS'
			
	  union
	
    select
      t.*,
      media_revenue."Pages Served",
      media_revenue."Net Media Revenue"
    from
      (
        select
          t.aggregation_level_date_in_et as Date,
          t.site_id,
          t.product_category_type,
          'Total' as segmentation_type,
          ifnull(t.pure_group_type, 'Total') as pure_group_type,
          sum(bookings_count * cookie_bias_adjustment_factor) as "Transactions",
          sum(net_conversion_sum * cookie_bias_adjustment_factor) as "Transaction Contribution"
        from intent_media_production.air_ct_transaction_performance_aggregations t
        group by
          t.aggregation_level_date_in_et,
          t.site_id,
          t.product_category_type,
          t.pure_group_type
      ) t
    left join
      (
        select
          aggregation_level_date_in_et,
          s.id as site_id,
          sum(ad_unit_served_count) as "Pages Served",
          sum(net_revenue_sum) as "Net Media Revenue"
        from intent_media_production.air_ct_media_performance_aggregations acmpa
        left join intent_media_production.ad_units au on acmpa.ad_unit_id = au.id
        left join intent_media_production.sites s on s.id = au.site_id
        group by
          aggregation_level_date_in_et,
          s.id
      ) media_revenue
      on t.site_id = media_revenue.site_id
        and t.Date = media_revenue.aggregation_level_date_in_et
        and t.segmentation_type in ('LOW_CONVERTING', 'Total')
        and t.pure_group_type in ('NOT_PURE','Total')
        and t.product_category_type = 'FLIGHTS'
	
	  union
	
    select
      t.*,
      media_revenue."Pages Served",
      media_revenue."Net Media Revenue"
    from
      (
        select
          t.aggregation_level_date_in_et as Date,
          t.site_id,
          t.product_category_type,
          ifnull(t.segmentation_type, 'Total') as segmentation_type,
          'Total' as pure_group_type,
          sum(bookings_count * cookie_bias_adjustment_factor) as "Transactions",
          sum(net_conversion_sum * cookie_bias_adjustment_factor) as "Transaction Contribution"
        from intent_media_production.air_ct_transaction_performance_aggregations t
        group by
          t.aggregation_level_date_in_et,
          t.site_id,
          t.product_category_type,
          t.segmentation_type
      ) t
    left join
      (
        select
          aggregation_level_date_in_et,
          s.id as site_id,
          sum(ad_unit_served_count) as "Pages Served",
          sum(net_revenue_sum) as "Net Media Revenue"
        from intent_media_production.air_ct_media_performance_aggregations acmpa
        left join intent_media_production.ad_units au on acmpa.ad_unit_id = au.id
        left join intent_media_production.sites s on s.id = au.site_id
        group by
          aggregation_level_date_in_et,
          s.id
      ) media_revenue
      on t.site_id = media_revenue.site_id
        and t.Date = media_revenue.aggregation_level_date_in_et
        and t.segmentation_type in ('LOW_CONVERTING', 'Total')
        and t.pure_group_type in ('NOT_PURE','Total')
        and t.product_category_type = 'FLIGHTS'
  ) t
on dimensions."Site ID" = t.site_id
	and dimensions."Segmentation Type" = t.segmentation_type
	and dimensions."Pure Group Type" = t.pure_group_type
	and dimensions.Date = t.Date
	and dimensions."Product Category Type" = t.product_category_type

left join intent_media_production.air_ct_unique_visitor_qtd_aggregations acuvqa
	on acuvqa.site_id = dimensions."Site ID"
	and ifnull(acuvqa.segmentation_type,'Total') = dimensions."Segmentation Type"
	and ifnull(acuvqa.pure_group_type,'Total') = dimensions."Pure Group Type"
	and acuvqa.date_in_et = dimensions.Date

left join intent_media_production.air_ct_unique_visitor_aggregations acuva
	on acuva.site_id = dimensions."Site ID"
	and ifnull(acuva.segmentation_type,'Total') = dimensions."Segmentation Type"
	and ifnull(acuva.pure_group_type,'Total') = dimensions."Pure Group Type"
	and acuva.date_in_et = dimensions.Date
	and acuva.lookback_window_type = 'THIRTY_DAYS'
	and acuva.destination is null
	
left join intent_media_production.sites s on s.id = dimensions."Site ID"

where dimensions.Date >= '2012-07-01'
  and (dimensions.Date < '2013-09-01' or dimensions.Date > '2013-09-30')