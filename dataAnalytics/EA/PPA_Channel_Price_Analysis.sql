------ First Query 
------ Basic Data Pull from Ad Calls, Impressions, Clicks


DROP TABLE IF EXISTS  Intent_media_sandbox_production.EA_ppa_price_compare_stage_1;
CREATE TABLE Intent_media_sandbox_production.EA_ppa_price_compare_stage_1
 AS

        SELECT a.request_id, a.ad_unit_id, hp.id as hotel_id, travelers, travel_date_end, travel_date_start, rooms, adults, children,
         a.publisher_ID,  a.requested_at_date_in_et, advertiser_ID, a.site_currency, 
                hotel_average_nightly_rate as advertiser_price, publisher_hotel_price  as publisher_price,      
             SUM(ISNULL(c.actual_cpc,0)) as revenue, count(c.actual_cpc) as clicks
        FROM intent_media_log_data_production.ad_calls a
        LEFT OUTER JOIN intent_media_production.hotel_properties hp  ON a.hotel_property_id = hp.id
        INNER JOIN intent_media_log_data_production.impressions i on i.request_id = a.request_id
        LEFT OUTER JOIN Intent_media_log_data_production.clicks c on i.external_id = c.external_impression_id
        WHERE ad_unit_type = 'META' and a.ip_address_blacklisted = false and outcome_type = 'SERVED' 
        and i.ip_address_blacklisted = false         
                AND hotel_average_nightly_rate > 0   ---- REMOVE THIS AND PUB > 0  AND HOTEL_PROPERTIES >0
                and publisher_hotel_price > 0 AND a.requested_at_date_in_et = to_date('02/11/2015','mm/dd/yyyy')
                AND (c.fraudulent = 'false' or c.fraudulent is null) AND (c.ip_address_blacklisted ='false' OR c.ip_address_blacklisted is null)
        GROUP BY a.request_id, a.ad_unit_id, a.publisher_id,    hotel_average_nightly_rate , publisher_hotel_price,     
           a.requested_at_date_in_et, advertiser_ID, a.site_currency,
             hp.id, travelers, travel_date_end, travel_date_start, rooms, adults, children ;

GRANT ALL on Intent_media_sandbox_production.EA_ppa_price_compare_stage_1 to PUBLIC;
 

------ Second Query 
------ Ads in data for names, Ad Units, and Sites.
------ The first inner query does this for the Advertisers
------ The second inner query that is unioned is just pulling the publisher data, and adding it as a row as if it were another advertiser.  The is_publisher field distinguishes between Advertisers and Publishers
------ The Outer query determines for each request_id the winning_price, whether each price is a win or a tie, or a loss, and then simply creates an order rank for each row
------ A win is defined as a price beating all other prices by more then $1
------ Since I'm only able to pull the minimum price, for each row I can only determine if the prices is within a dollar of that price so also a win, but I don't know if other prices are also within a dollar
------ so I call it a win_or_tie, and will figure out which later

DROP TABLE IF EXISTS  Intent_media_sandbox_production.EA_ppa_price_compare_stage_2;
CREATE TABLE Intent_media_sandbox_production.EA_ppa_price_compare_stage_2
AS

------LOOK INTO CONDITIONAL true logic via Sharon Cutter
SELECT a.*, Min(advertiser_price) OVER (Partition BY request_ID) as winning_Price, 
case WHEN advertiser_price -  Min(advertiser_price) OVER (Partition BY request_ID) < 1 Then 1 ELSE 0 END as win_or_tie,
case WHEN advertiser_price -  Min(advertiser_price) OVER (Partition BY request_ID) < 1 Then 0 ELSE 1 END as loss,
RANK() OVER (PARTITION BY request_ID ORDER BY Advertiser) as torder

FROM 
        (
        
        --------Advertisers
        SELECT request_ID,  a.publisher_id,
        
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
          
          ---normalizes display names between Publishers and Advertisers which are different entity_IDs for the same company.  Removes capitals and .com
          CONCAT(LEFT(s.display_name,1) , LOWER(Right(s.display_name, length(s.display_name) -1))) as site, 
          ad.name as "Ad Unit",  a.ad_unit_id, lp.page_type,  a.requested_at_date_in_et, a.site_currency, 
          revenue, clicks, a.advertiser_id,
          REPLACE(CONCAT(LEFT( e1.ad_display_name,1), LOWER(Right(e1.ad_display_name, length( e1.ad_display_name) -1))),'.com','') as Advertiser, 
           0 as is_publisher,  hotel_id, travelers, travel_date_end, travel_date_start, rooms, adults, children,
          advertiser_price ,   
         
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
                         
                
        FROM Intent_media_sandbox_production.EA_ppa_price_compare_stage_1 a
        INNER JOIN intent_media_production.ad_units ad on ad.id = a.ad_unit_id
        INNER JOIN intent_media_production.sites s on ad.site_ID = s.ID
        INNER JOIN intent_media_production.legal_page_types lp on lp.id = ad.legal_page_type_id
        LEFT JOIN (SELECT id, case WHEN ad_display_name is null then name else ad_display_name END as ad_display_name from intent_media_production.entities) e1 on e1.ID = a.advertiser_ID
        LEFT JOIN (SELECT id, case WHEN ad_display_name is null then name else ad_display_name END as ad_display_name from intent_media_production.entities) e2 on e2.ID = a.publisher_id
        
        UNION
        
        --Publishers
        SELECT DISTINCT request_ID,  a.publisher_id,
        
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
        ad.name as "Ad Unit",  a.ad_unit_id, lp.page_type, a.requested_at_date_in_et, a.site_currency, 
        0 as revenue, 0 as clicks, a.publisher_id,
        REPLACE(CONCAT(LEFT(s.display_name,1) , LOWER(Right(s.display_name, length(s.display_name) -1))),'.com','') as Advertiser, 
         1 as is_publisher,   hotel_id, travelers, travel_date_end, travel_date_start, rooms, adults, children,
        publisher_price as advertiser_price,   
                     
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
        else e2.ad_display_name end as Cartel
        
        FROM Intent_media_sandbox_production.EA_ppa_price_compare_stage_1 a
        INNER JOIN intent_media_production.ad_units ad on ad.id = a.ad_unit_id
        INNER JOIN intent_media_production.sites s on ad.site_ID = s.ID
        INNER JOIN intent_media_production.legal_page_types lp on lp.id = ad.legal_page_type_id
        LEFT JOIN (SELECT id, case WHEN ad_display_name is null then name else ad_display_name END as ad_display_name from intent_media_production.entities) e1 on e1.ID = a.advertiser_ID
        LEFT JOIN (SELECT id, case WHEN ad_display_name is null then name else ad_display_name END as ad_display_name from intent_media_production.entities) e2 on e2.ID = a.publisher_id) a;


GRANT ALL on Intent_media_sandbox_production.EA_ppa_price_compare_stage_2 to PUBLIC;







SELECT *
FROM Intent_media_sandbox_production.EA_ppa_price_compare_stage_2
WHERE hotel_id = 62515 AND travelers = 2 AND travel_date_end = '2015-02-15' AND travel_date_start = '2015-02-14' 
AND rooms = 1 AND adults = 2 AND children = 0


SELECT *
FROM Intent_media_sandbox_production.EA_ppa_competition_stage_1 



SELECT *
FROM  Intent_media_sandbox_production.EA_ppa_price_compare

DROP TABLE IF EXISTS Intent_media_sandbox_production.EA_ppa_price_compare


---Aggregate Entity against itself as both Pub and Adv
CREATE TABLE Intent_media_sandbox_production.EA_ppa_price_compare
AS

SELECT pub.Advertiser, 
count(adv.request_ID) as comparisons,
SUM(CASE WHEN pub.advertiser_price - adv.advertiser_price <= -1 Then 1 ELSE 0 END) AS num_of_wins,
SUM(CASE WHEN pub.advertiser_price - adv.advertiser_price >= 1 Then 1 ELSE 0 END) As num_of_losses,
SUM(CASE WHEN pub.advertiser_price - adv.advertiser_price < 1 AND pub.advertiser_price - adv.advertiser_price > -1 Then 1 ELSE 0 END) AS Tie,

SUM(case WHEN pub.advertiser_price - adv.advertiser_price <= -1 Then (adv.advertiser_price - pub.advertiser_price)/pub.advertiser_price ELSE null END) as WINS_SUM_PRCT_Differences_Adv_to_Pub,    
SUM(case WHEN pub.advertiser_price - adv.advertiser_price >= 1 Then (adv.advertiser_price - pub.advertiser_price)/pub.advertiser_price ELSE null END) as Loss_SUM_PRCT_Differences_Adv_to_Pub,
SUM((adv.advertiser_price - pub.advertiser_price)/pub.advertiser_price) as SUM_PCT_Differences_Adv_to_Pub

FROM

        (SELECT DISTINCT Advertiser, travel_date_end, travel_date_start, rooms, adults, children, hotel_id, travelers, advertiser_price, count(DISTINCT request_ID) as ad_calls
        FROM Intent_media_sandbox_production.EA_ppa_price_compare_stage_2
        WHERE is_publisher = 1
        GROUP BY  Advertiser, travel_date_end, travel_date_start, rooms, adults, children, hotel_id, travelers, advertiser_price ) pub
INNER JOIN

        (SELECT *
        FROM Intent_media_sandbox_production.EA_ppa_price_compare_stage_2
        WHERE is_publisher = 0) adv
        
ON  pub.Advertiser = adv.Advertiser 
AND pub.travel_date_end = adv.travel_date_end 
AND pub.travel_date_start = adv.travel_date_start 
AND pub.rooms = adv.rooms 
AND pub.adults = adv.adults 
AND pub.children = adv.children 
AND pub.hotel_id = adv.hotel_id
AND pub.travelers = adv.travelers
WHERE ABS((adv.advertiser_price - pub.advertiser_price)/pub.advertiser_price) <= .5 
GROUP BY pub.Advertiser



---- BREAK DOWN BY MARKET BUCKETS!???

SELECT *
FROM  Intent_media_sandbox_production.EA_ppa_price_compare_raw_All

---raw
DROP TABLE IF EXISTS Intent_media_sandbox_production.EA_ppa_price_compare_raw_All;
CREATE TABLE Intent_media_sandbox_production.EA_ppa_price_compare_raw_All
AS

Select Pub.Advertiser as Advertiser_1, cast(pub.advertiser_price as float) as adv_1_price, adv.Advertiser as Advertiser_2, cast(adv.advertiser_price as float) as adv_2_price,  
cast((adv.advertiser_price - pub.advertiser_price)/pub.advertiser_price as float) as pct_diff, requested_at_date_in_et
---count(*)
FROM

        (SELECT  Advertiser, is_publisher, travel_date_end, travel_date_start, rooms, adults, children, hotel_id, travelers, advertiser_price, 
        requested_at_date_in_et, requested_at_in_et
        FROM Intent_media_sandbox_production.EA_ppa_competition_stage_2
        WHERE is_publisher = 1 
        GROUP BY  Advertiser, is_publisher, travel_date_end, travel_date_start, rooms, adults, children, hotel_id, travelers, advertiser_price ) pub
INNER JOIN

        (SELECT *
        FROM Intent_media_sandbox_production.EA_ppa_competition_stage_2
        WHERE is_publisher = 0 
        ) adv
        
ON  pub.Advertiser = adv.Advertiser 
AND pub.travel_date_end = adv.travel_date_end 
AND pub.travel_date_start = adv.travel_date_start 
AND pub.rooms = adv.rooms 
AND pub.adults = adv.adults 
AND pub.children = adv.children 
AND pub.hotel_id = adv.hotel_id
AND pub.travelers = adv.travelers
WHERE NOT (pub.Advertiser = adv.Advertiser AND pub.is_publisher = adv.is_publisher)
AND ABS((adv.advertiser_price - pub.advertiser_price)/pub.advertiser_price) <= .5 
AND ABS(pub.requested_at_in_et = adv.requested_at_in_et) <= '02:00:00' ;


GRANT ALL on Intent_media_sandbox_production.EA_ppa_price_compare_raw_All to PUBLIC;