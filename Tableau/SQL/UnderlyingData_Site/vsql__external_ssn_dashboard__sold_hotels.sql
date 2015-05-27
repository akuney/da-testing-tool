select 
	sold_hotels.Date,
	sold_hotels."SSN Channel Type",
	sold_hotels."Sold Hotels",
	budgets."Budget"
from
(
  select
		dates.date_in_et as Date,
		dates.ssn_channel_type as "SSN Channel Type",
		sold_hotels_query.sold_hotels as "Sold Hotels"		
  from
  (
    select *
    from
    (
      select
        distinct(aggregation_level_date_in_et) as date_in_et
      from intent_media_production.participating_advertisers
    ) dates,
    (
      select
        distinct(ssn_channel_type) as ssn_channel_type
      from intent_media_production.entities
      where ssn_channel_type is not null
    ) ssn_channel_types
  ) dates
  left join
  (
    select
      sold_date_in_et as date_in_et,
      ssn_channel_type,
      count(distinct(id)) as sold_hotels
    from
    (
      select
        id,
        ssn_channel_type,
        DATE(first_auction_participation at timezone 'America/New_York') as sold_date_in_et
      from intent_media_production.entities
      where entity_type = 'HotelSsrAdvertiser'
        and active = 1
        and first_auction_participation is not null
    ) sold_dates
  group by
    date_in_et,
    ssn_channel_type
  ) sold_hotels_query
  on sold_hotels_query.date_in_et = dates.date_in_et
  and sold_hotels_query.ssn_channel_type = dates.ssn_channel_type
) sold_hotels
left join
(
  select
	  pa.Date,
	  pa."SSN Channel Type",
	  sum(budgets_query.Budget) as Budget
  from
	(
    select
      aggregation_level_date_in_et as Date,
      e.ssn_channel_type as "SSN Channel Type",
      advertiser_id as "Advertiser ID"
    from intent_media_production.participating_advertisers p
    left join intent_media_production.entities e on e.id = p.advertiser_id
    group by
      aggregation_level_date_in_et,
      e.ssn_channel_type,
      advertiser_id
  ) pa
  left join
	(
    select
      latest.date_in_et,
      latest.ssn_channel_type,
      latest.advertiser_id,
      allocated_budget as "Budget"
    from
    (
      select
        date_in_et,
        e.ssn_channel_type,
        advertiser_id,
        max(h.id) as latest_id
      from intent_media_production.historical_budgets h
      left join intent_media_production.entities e on e.id = h.advertiser_id
      group by
        date_in_et,
        e.ssn_channel_type,
        advertiser_id
    ) latest
		left join intent_media_production.historical_budgets hb 
		on latest.date_in_et = hb.date_in_et 
			and latest.advertiser_id = hb.advertiser_id
			and latest.latest_id = hb.id
  ) budgets_query
  on budgets_query.date_in_et = pa.Date
    and budgets_query.ssn_channel_type = pa."SSN Channel Type"
    and budgets_query.advertiser_id = pa."Advertiser ID"
  group by Date, "SSN Channel Type"
) budgets
on budgets.Date = sold_hotels.Date
and budgets."SSN Channel Type" = sold_hotels."SSN Channel Type"
where sold_hotels.Date < date(current_timestamp at timezone 'America/New_York')