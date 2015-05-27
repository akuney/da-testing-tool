select
	dimensions.Date,
	dimensions."Site ID",
	dimensions."Site",
	(case dimensions.product_category_type 
		when 'FLIGHTS' then 'Flights'
		when 'HOTELS' then 'Hotels'
		when 'PACKAGES' then 'Packages'
		when 'CARS' then 'Cars'
		else dimensions.product_category_type
	end) as "Product Category Type",
	(case dimensions.segmentation_type
		when 'HIGH_CONVERTING' then 'High Value'
		when 'LOW_CONVERTING' then 'Low Value'
		else dimensions.segmentation_type
	end) as "Segmentation Type",
	(case dimensions.pure_group_type
		when 'PURE' then 'Pure'
		when 'NOT_PURE' then 'Not Pure'
		else dimensions.pure_group_type
	end) as "Pure Group Type",
	acuvqa.unique_visitors as "Unique Visitors",
	performance."Transactions",
	performance."Transaction Contribution",
	performance."Pages Served",
	performance."Net Media Revenue"
from
	(
    select
      dates.Date,
      t.site_id as "Site ID",
      t.display_name as "Site",
      t.product_category_type,
      ifnull(t.pure_group_type,'Total') as pure_group_type,
      ifnull(t.segmentation_type,'Total') as segmentation_type
    from
    (select
      distinct(aggregation_level_date_in_et) as Date
    from intent_media_production.air_ct_transaction_performance_aggregations
    where aggregation_level_date_in_et >= '2012-06-01') dates,
    (
      select
        s.display_name,
        actpa.site_id,
        actpa.product_category_type,
        ifnull(actpa.segmentation_type,'Total') as segmentation_type,
        ifnull(actpa.pure_group_type, 'Total') as pure_group_type
      from intent_media_production.air_ct_transaction_performance_aggregations actpa
      left join intent_media_production.sites s on s.id = actpa.site_id
      where actpa.site_id in (2,3,4)
      group by
        s.display_name,
        actpa.site_id,
        actpa.product_category_type,
        actpa.segmentation_type,
        actpa.pure_group_type

      union

      select
        s.display_name,
        actpa.site_id,
        actpa.product_category_type,
        'Total' as segmentation_type,
        ifnull(actpa.pure_group_type, 'Total') as pure_group_type
      from intent_media_production.air_ct_transaction_performance_aggregations actpa
      left join intent_media_production.sites s on s.id = actpa.site_id
      where actpa.site_id in (2,3,4)
      group by
        s.display_name,
        actpa.site_id,
        actpa.product_category_type,
        actpa.pure_group_type

      union

      select
          s.display_name,
          actpa.site_id,
          actpa.product_category_type,
          ifnull(actpa.segmentation_type,'Total') as segmentation_type,
          'Total' as pure_group_type
        from intent_media_production.air_ct_transaction_performance_aggregations actpa
        left join intent_media_production.sites s on s.id = actpa.site_id
        where actpa.site_id in (2,3,4)
        group by
          s.display_name,
          actpa.site_id,
          actpa.product_category_type,
          actpa.segmentation_type
    ) t
  ) dimensions
		
		
left join
	-- performance data
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
        group by t.aggregation_level_date_in_et, t.site_id, t.segmentation_type, t.pure_group_type, t.product_category_type
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
      (select
        t.aggregation_level_date_in_et as Date,
        t.site_id,
        t.product_category_type,
        'Total' as segmentation_type,
        ifnull(t.pure_group_type, 'Total') as pure_group_type,
        sum(bookings_count * cookie_bias_adjustment_factor) as "Transactions",
        sum(net_conversion_sum * cookie_bias_adjustment_factor) as "Transaction Contribution"
      from intent_media_production.air_ct_transaction_performance_aggregations t
      group by t.aggregation_level_date_in_et, t.site_id, t.pure_group_type, t.product_category_type) t

    left join
      (select
        aggregation_level_date_in_et,
        s.id as site_id,
        sum(ad_unit_served_count) as "Pages Served",
        sum(net_revenue_sum) as "Net Media Revenue"
      from intent_media_production.air_ct_media_performance_aggregations acmpa
      left join intent_media_production.ad_units au on acmpa.ad_unit_id = au.id
      left join intent_media_production.sites s on s.id = au.site_id
      group by
        aggregation_level_date_in_et,
        s.id) media_revenue
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
      (select
        t.aggregation_level_date_in_et as Date,
        t.site_id,
        t.product_category_type,
        ifnull(t.segmentation_type, 'Total') as segmentation_type,
        'Total' as pure_group_type,
        sum(bookings_count * cookie_bias_adjustment_factor) as "Transactions",
        sum(net_conversion_sum * cookie_bias_adjustment_factor) as "Transaction Contribution"
      from intent_media_production.air_ct_transaction_performance_aggregations t
      group by t.aggregation_level_date_in_et, t.site_id, t.segmentation_type, t.product_category_type) t

    left join
      (select
        aggregation_level_date_in_et,
        s.id as site_id,
        sum(ad_unit_served_count) as "Pages Served",
        sum(net_revenue_sum) as "Net Media Revenue"
      from intent_media_production.air_ct_media_performance_aggregations acmpa
      left join intent_media_production.ad_units au on acmpa.ad_unit_id = au.id
      left join intent_media_production.sites s on s.id = au.site_id
      group by
        aggregation_level_date_in_et,
        s.id) media_revenue
      on t.site_id = media_revenue.site_id
        and t.Date = media_revenue.aggregation_level_date_in_et
        and t.segmentation_type in ('LOW_CONVERTING', 'Total')
        and t.pure_group_type in ('NOT_PURE','Total')
        and t.product_category_type = 'FLIGHTS'
  ) performance
			
	on performance.site_id = dimensions."Site ID"
    and performance.segmentation_type = dimensions.segmentation_type
    and performance.pure_group_type = dimensions.pure_group_type
    and performance.product_category_type = dimensions.product_category_type
    and performance.Date = dimensions.Date
	
  left join
    (
      select
        site_id,
        ifnull(segmentation_type,'Total') as segmentation_type,
        ifnull(pure_group_type,'Total') as pure_group_type,
        date_in_et,
        sum(count) as unique_visitors
      from intent_media_production.air_ct_unique_visitor_qtd_aggregations
      group by
        site_id,
        ifnull(segmentation_type,'Total'),
        ifnull(pure_group_type,'Total'),
        date_in_et
    ) acuvqa

  on acuvqa.site_id = dimensions."Site ID"
    and ifnull(acuvqa.segmentation_type,'Total') = dimensions.segmentation_type
    and ifnull(acuvqa.pure_group_type,'Total') = dimensions.pure_group_type
    and acuvqa.date_in_et = dimensions.Date

where dimensions.Date >= '2012-07-01'
  and (dimensions.Date < '2013-09-01' or dimensions.Date > '2013-09-30')