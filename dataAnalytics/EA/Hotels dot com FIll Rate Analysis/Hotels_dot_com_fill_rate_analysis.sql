        
DROP TABLE IF EXISTS  Intent_media_sandbox_production.EA_ppa_hotels_fill_rate1;
CREATE TABLE Intent_media_sandbox_production.EA_ppa_hotels_fill_rate1
 AS

        SELECT a.request_id, a.ad_unit_id, a.requested_at_date_in_et, i.advertiser_ID, a.site_currency, number_of_advertisers_in_auction,  i.ip_address_blacklisted , outcome_type, a.publisher_id, 'served' as filter_cause_type
        FROM intent_media_log_data_production.ad_calls a
        LEFT OUTER JOIN intent_media_log_data_production.impressions i on i.request_id = a.request_id
        WHERE ad_unit_type = 'META' and a.ip_address_blacklisted = false AND a.Publisher_ID = 109
        AND a.requested_at_date_in_et = to_date('12/30/2014','mm/dd/yyyy') 
        
        UNION

        SELECT a.request_id, a.ad_unit_id, a.requested_at_date_in_et, i.advertiser_ID, a.site_currency, number_of_advertisers_in_auction,  i.ip_address_blacklisted , outcome_type, a.publisher_id, filter_cause_type
        FROM intent_media_log_data_production.ad_calls a
        LEFT OUTER JOIN intent_media_log_data_production.filtered_advertisements i on i.request_id = a.request_id
        WHERE ad_unit_type = 'META' and a.ip_address_blacklisted = false AND a.Publisher_ID = 109
        AND a.requested_at_date_in_et = to_date('12/30/2014','mm/dd/yyyy')       


    
------2


DROP TABLE IF EXISTS  Intent_media_sandbox_production.EA_ppa_hotels_fill_rate2;
CREATE TABLE Intent_media_sandbox_production.EA_ppa_hotels_fill_rate2
AS

SELECT a.*, RANK() OVER (PARTITION BY request_ID ORDER BY Advertiser) as torder

FROM 

        (SELECT request_ID,  a.publisher_id, number_of_advertisers_in_auction,  ip_address_blacklisted , outcome_type, filter_cause_type,
        
          case
	    when e2.ad_display_name = 'Air Fast Tickets' then 'AirFastTickets'
	    when e2.ad_display_name = 'Amoma' then 'AMOMA.com'
	    when e2.ad_display_name = 'Bookit' then 'BookIt.com'
	    when e2.ad_display_name = 'Expedia' then 'Expedia Inc.'
	    when e2.ad_display_name = 'Hotwire' then 'Expedia Inc.' 
	    when e2.ad_display_name = 'Travelocity on Expedia' then 'Expedia Inc.' 
	    when e2.ad_display_name = 'Travelocity' then 'Expedia Inc.'		    
	    when e2.ad_display_name = 'Kayak Software Corporation' then 'KAYAK' 
	    when e2.ad_display_name = 'lastminute.com' then 'Sabre Holdings' 
	    when e2.ad_display_name = 'Orbitz' then 'Orbitz Worldwide, Inc.'
	    when e2.ad_display_name = 'Oversee' then 'Oversee.net'
	    when e2.ad_display_name = 'Hotels.com' then 'Expedia Inc.'
	    when e2.ad_display_name = 'Trivago' then 'Expedia Inc.'
	    when e2.ad_display_name = 'Cheaptickets' then 'Orbitz Worldwide, Inc.' 
	    when e2.ad_display_name = 'ebookers.com' then 'Orbitz Worldwide, Inc.'	
	    when e2.ad_display_name = 'ebookers' then 'Orbitz Worldwide, Inc.'		        	        
	    when e2.ad_display_name = 'Booking.com' then 'Priceline'			        	    
          else e2.ad_display_name end as Publisher,
                
          CONCAT(LEFT(s.display_name,1) , LOWER(Right(s.display_name, length(s.display_name) -1))) as site, 
          ad.name as "Ad Unit",  a.ad_unit_id, lp.page_type,  a.requested_at_date_in_et,  a.site_currency, a.advertiser_id,
          REPLACE(CONCAT(LEFT( e1.ad_display_name,1), LOWER(Right(e1.ad_display_name, length( e1.ad_display_name) -1))),'.com','') as Advertiser, 
          0 as is_publisher,
         
          case
	    when e1.ad_display_name = 'Air Fast Tickets' then 'AirFastTickets'
	    when e1.ad_display_name = 'Amoma' then 'AMOMA.com'
	    when e1.ad_display_name = 'Bookit' then 'BookIt.com'
	    when e1.ad_display_name = 'Expedia' then 'Expedia Inc.'
	    when e1.ad_display_name = 'Hotwire' then 'Expedia Inc.' 
	    when e1.ad_display_name = 'Travelocity on Expedia' then 'Expedia Inc.' 
	    when e1.ad_display_name = 'Travelocity' then 'Expedia Inc.'		    
	    when e1.ad_display_name = 'Kayak Software Corporation' then 'KAYAK' 
	    when e1.ad_display_name = 'lastminute.com' then 'Sabre Holdings' 
	    when e1.ad_display_name = 'Orbitz' then 'Orbitz Worldwide, Inc.'
	    when e1.ad_display_name = 'Oversee' then 'Oversee.net'
	    when e1.ad_display_name = 'Hotels.com' then 'Expedia Inc.'
	    when e1.ad_display_name = 'Trivago' then 'Expedia Inc.'	
	    when e1.ad_display_name = 'Cheaptickets' then 'Orbitz Worldwide, Inc.' 	
	    when e1.ad_display_name = 'ebookers.com' then 'Orbitz Worldwide, Inc.'
	    when e1.ad_display_name = 'ebookers' then 'Orbitz Worldwide, Inc.'	    		   
	    when e1.ad_display_name = 'Booking.com' then 'Priceline'			    	         		    
          else e1.ad_display_name end as Cartel
                         
                
        FROM Intent_media_sandbox_production.EA_ppa_hotels_fill_rate1 a
        INNER JOIN intent_media_production.ad_units ad on ad.id = a.ad_unit_id
        INNER JOIN intent_media_production.sites s on ad.site_ID = s.ID
        INNER JOIN intent_media_production.legal_page_types lp on lp.id = ad.legal_page_type_id
        LEFT JOIN (SELECT id, case WHEN ad_display_name is null then name else ad_display_name END as ad_display_name from intent_media_production.entities) e1 on e1.ID = a.advertiser_ID
        LEFT JOIN (SELECT id, case WHEN ad_display_name is null then name else ad_display_name END as ad_display_name from intent_media_production.entities) e2 on e2.ID = a.publisher_id
        WHERE a.advertiser_ID is not null
        ) a;
 
 
 
-------------------3 

DROP TABLE IF EXISTS Intent_media_sandbox_production.EA_ppa_hotels_fill_rate_12302014;
CREATE TABLE Intent_media_sandbox_production.EA_ppa_hotels_fill_rate_12302014
AS

SELECT request_ID, number_of_advertisers_in_auction, "Ad Unit" , requested_at_date_in_et, site_currency,
SUM(CASE filter_cause_type WHEN 'served' THEN 1 ELSE 0 END) as served_impressions,
SUM(CASE WHEN filter_cause_type = 'served' AND Cartel = 'Expedia Inc.' THEN 1 ELSE 0 END) as House_Brands_Served,
SUM(CASE WHEN filter_cause_type = 'served' AND Cartel <> 'Expedia Inc.' THEN 1 ELSE 0 END) as Non_House_Brands_Served,
SUM(CASE filter_cause_type WHEN 'ADVERTISER_MINIMUM_NOT_MET' THEN 1 ELSE 0 END) as unserved_impressions,
SUM(CASE WHEN filter_cause_type = 'ADVERTISER_MINIMUM_NOT_MET' AND Cartel = 'Expedia Inc.' THEN 1 ELSE 0 END) as unserved_House_Brand_impressions,
SUM(CASE WHEN filter_cause_type = 'ADVERTISER_MINIMUM_NOT_MET' AND Cartel <> 'Expedia Inc.' THEN 1 ELSE 0 END) as unserved_Non_House_Brand_impressions
FROM Intent_media_sandbox_production.EA_ppa_hotels_fill_rate2
WHERE ip_address_blacklisted = false
GROUP BY   request_ID, number_of_advertisers_in_auction, "Ad Unit", requested_at_date_in_et, site_currency;