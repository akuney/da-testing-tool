-- specific advertiser Fill in your advertiser_name below

select sum(impressions) as "impressions", 
sum(impressions)/sum(pages_served) as "Impression Share", 
sum(auction_position_sum)/sum(impressions) as "Average Auction Position",
sum(clicks)/sum(impressions) as "CTR",
sum(spend)/sum(clicks) as "CPC",
sum(spend) as "Spend"  
from air_ct_daily_dashboard_datasets 
where 
advertiser_name='CheapOair' and 
date between '20120715' and '20120721';

-- Advertiser segment for Tier 1: need to comment out specific Tier 1 advertiser below

select 
avg(`avg impressions`),
avg(`avg impression share`),
avg(`Average Auction Position`),
avg(`Average CTR`),
avg(`Average CPC`),
avg(`Average Spend`) 
from 
(select
	advertiser_name,
	sum(impressions) as "avg impressions",
	sum(impressions)/sum(pages_served) as "avg impression share",
	sum(auction_position_sum)/sum(impressions) as "Average Auction Position",
	sum(clicks)/sum(impressions) as "Average CTR" ,
	sum(spend)/sum(clicks) as "Average CPC" ,
	sum(spend) as "Average Spend" 
from air_ct_daily_dashboard_datasets 
where advertiser_name in ('Priceline','Travelocity-Ads','Hotwire'/*,'Expedia-Ads'*/) 
and dataset_type = 'ADVERTISER' 
and date between '20120715' and '20120721'
and impressions > 0
group by advertiser_name) by_advertiser;

-- benchmarks advertiser segment for Tier 1: need to comment out specific Tier 1 advertiser below

select 
2 * stddev(`sum impressions`) as "std impressions",
avg(`sum impressions`) - stddev(`sum impressions`) as "lci impressions",
2 * stddev(`sum impression share`) as "std impression share",
avg(`sum impression share`) - stddev(`sum impression share`) as "lci impression share",
2 * stddev(`sum Average Auction Position`) as "std Average Auction Position",
avg(`sum Average Auction Position`) - stddev(`sum Average Auction Position`) as "lci Average Auction Position",
2 * stddev(`sum Average CTR`) as "std Average CTR",
avg(`sum Average CTR`) - stddev(`sum Average CTR`) as "lci Average CTR",
2 * stddev(`sum Average CPC`) as "std Average CPC",
avg(`sum Average CPC`) - stddev(`sum Average CPC`) as "lci Average CPC",
2 * stddev(`sum Average Spend`) as "std Average Spend",
avg(`sum Average Spend`) - stddev(`sum Average Spend`) as "lci Average Spend"
from 
(select
	advertiser_name,
	sum(impressions) as "sum impressions",
	sum(impressions)/sum(pages_served) as "sum impression share",
	sum(auction_position_sum)/sum(impressions) as "sum Average Auction Position",
	sum(clicks)/sum(impressions) as "sum Average CTR" ,
	sum(spend)/sum(clicks) as "sum Average CPC" ,
	sum(spend) as "sum Average Spend" 
from air_ct_daily_dashboard_datasets 
where advertiser_name in ('Priceline'/*,'Travelocity-Ads'*/,'Hotwire','Expedia-Ads') 
and dataset_type = 'ADVERTISER' 
and date between '20120715' and '20120721'
and impressions > 0
group by advertiser_name) by_advertiser;

-- Advertiser segment Tier 2: need to all of Tier 1 and the specific Tier 2 advertiser below

select 
avg(`avg impressions`),
avg(`avg impression share`),
avg(`Average Auction Position`),
avg(`Average CTR`),
avg(`Average CPC`),
avg(`Average Spend`) 
from 
(select
	advertiser_name,
	sum(impressions) as "avg impressions",
	sum(impressions)/sum(pages_served) as "avg impression share",
	sum(auction_position_sum)/sum(impressions) as "Average Auction Position",
	sum(clicks)/sum(impressions) as "Average CTR" ,
	sum(spend)/sum(clicks) as "Average CPC" ,
	sum(spend) as "Average Spend" 
from air_ct_daily_dashboard_datasets 
where advertiser_name not in ('Priceline','Travelocity-Ads','Hotwire','Expedia-Ads', 'CheapOair') 
and dataset_type = 'ADVERTISER' 
and date between '20120715' and '20120721'
and impressions > 0
and advertiser_segment='OTA'
group by advertiser_name) by_advertiser;

-- benchmarks advertiser segment tier 2: need to all of Tier 1 and the specific Tier 2 advertiser below

select 
2 * stddev(`sum impressions`) as "std impressions",
avg(`sum impressions`) - stddev(`sum impressions`) as "lci impressions",
2 * stddev(`sum impression share`) as "std impression share",
avg(`sum impression share`) - stddev(`sum impression share`) as "lci impression share",
2 * stddev(`sum Average Auction Position`) as "std Average Auction Position",
avg(`sum Average Auction Position`) - stddev(`sum Average Auction Position`) as "lci Average Auction Position",
2 * stddev(`sum Average CTR`) as "std Average CTR",
avg(`sum Average CTR`) - stddev(`sum Average CTR`) as "lci Average CTR",
2 * stddev(`sum Average CPC`) as "std Average CPC",
avg(`sum Average CPC`) - stddev(`sum Average CPC`) as "lci Average CPC",
2 * stddev(`sum Average Spend`) as "std Average Spend",
avg(`sum Average Spend`) - stddev(`sum Average Spend`) as "lci Average Spend"
from 
(select
	advertiser_name,
	sum(impressions) as "sum impressions",
	sum(impressions)/sum(pages_served) as "sum impression share",
	sum(auction_position_sum)/sum(impressions) as "sum Average Auction Position",
	sum(clicks)/sum(impressions) as "sum Average CTR" ,
	sum(spend)/sum(clicks) as "sum Average CPC" ,
	sum(spend) as "sum Average Spend" 
from air_ct_daily_dashboard_datasets 
where advertiser_name not in ('Priceline','Travelocity-Ads','Hotwire','Expedia-Ads', 'CheapOair') 
and dataset_type = 'ADVERTISER' 
and date between '20120715' and '20120721'
and impressions > 0
and advertiser_segment='OTA'
group by advertiser_name) by_advertiser;


