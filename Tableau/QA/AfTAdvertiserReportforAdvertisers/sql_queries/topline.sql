-- TOPLINE

select sum(impressions) as "impressions",
sum(pages_served) as "pages served",
sum(pages_served)-sum(impressions) as "impressions vs pages served",   
sum(impressions)/sum(pages_served) as "Impression Share", 
sum(auction_position_sum)/sum(impressions) as "Average Auction Position",
sum(clicks)/sum(impressions) as "CTR",
sum(spend)/sum(clicks) as "CPC",
sum(spend) as "Spend"  
from air_ct_daily_dashboard_datasets 
where 
advertiser_name='Orbitz-ads-on-EXPE' and 
date between '20120422' and '20120428';

-- ROI
select sum(click_conversion_count)/sum(click_count) as "CVR",
sum(click_conversion_value_sum) as "Revenue", 
sum(actual_cpc_sum)/sum(click_conversion_count) as "CPA",
sum(click_conversion_value_sum)/sum(actual_cpc_sum) as "ROI",
sum(actual_cpc_sum)/sum(click_count) as "CPC",
sum(actual_cpc_sum) as "Spend"
from air_ct_advertiser_performance_report_aggregations 
where 
advertiser_id in (select id from entities where name='Orbitz-ads-on-EXPE') and 
date_in_et between '20120422' and '20120428';

