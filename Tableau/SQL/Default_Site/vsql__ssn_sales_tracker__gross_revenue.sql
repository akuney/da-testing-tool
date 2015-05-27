select
	aara.aggregation_level_date_in_et as Date,
	sum(aara.actual_cpc_sum) as Revenue,
	sum(case when e.ssn_channel_type = 'OTA' then aara.actual_cpc_sum end) as Revenue_ota,
	sum(case when e.ssn_channel_type = 'GDS' then aara.actual_cpc_sum end) as Revenue_gds,
	sum(case when imm.report_segment = 'Global Top 10' or imm.report_segment = 'Global 11 to 50' then aara.actual_cpc_sum end) as "Revenue - Global Top 50"
from intent_media_production.advertiser_account_report_aggregations aara
left join intent_media_production.intent_media_markets_publisher_markets immpm on immpm.market_id = aara.market_id
left join intent_media_production.intent_media_markets imm on imm.id = immpm.intent_media_market_id
left join intent_media_production.entities e on e.id = aara.advertiser_id
where aggregation_level_date_in_et < date(current_timestamp at timezone 'America/New_York')
group by aggregation_level_date_in_et