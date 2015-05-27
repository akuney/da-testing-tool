import datetime
import sys


import subprocess

# credential to connect production Vertica clusters
host = 'production-vertica-cluster-with-failover.internal.intentmedia.net'
user_name = 'Tableau'
password = '9WOGfffN'


 # to_date('""" + strmonth + strday + stryear + """','mm/dd/yyyy')

def do_analysis1(stryear, strmonth, strday):

    print "infunction_1"
    print strmonth + strday + stryear

    statement = """ 


        DELETE FROM  Intent_media_sandbox_production.EA_ppa_competition_stage_1; COMMIT; 
        INSERT INTO  Intent_media_sandbox_production.EA_ppa_competition_stage_1


         SELECT case WHEN publisher_price -  Min(advertiser_price) OVER (Partition BY a.request_ID) <= 1 AND
                          publisher_price -  MIN(advertiser_price) OVER (Partition BY a.request_ID) >= -1 THEN 1 ELSE 0 END as Pub_tie,
                          
               case WHEN publisher_price -  Min(advertiser_price) OVER (Partition BY a.request_ID) > 1 THEN 1 ELSE 0 END as Pub_loss,
               case WHEN publisher_price -  Min(advertiser_price) OVER (Partition BY a.request_ID) < -1 THEN 1 ELSE 0 END as Pub_win,
               a.*
          FROM
          (
          SELECT a.request_id, a.ad_unit_id, auction_position, 
          travelers, travel_date_end, travel_date_start, rooms, adults, children, a.requested_at_in_et,
           a.publisher_ID,  a.requested_at_date_in_et, advertiser_ID, a.site_currency, 
                 case WHEN a.rank_in_page <= 5 THEN 'TOP 5' ELSE 'NOT TOP 5'END as rank_in_page,
                 CASE WHEN hp.country IN ('United States', 'US', 'USA') THEN 'Domestic' 
                      WHEN hp.country is null  THEN 'Unknown' 
                      ELSE 'International' END As Dom_Intl,
               CAST(CASE WHEN is_per_person_rate = TRUE AND is_per_night_rate = FALSE THEN hotel_average_nightly_rate*travelers/(travel_date_end - travel_date_start)
               WHEN is_per_person_rate = TRUE AND is_per_night_rate = TRUE THEN hotel_average_nightly_rate*travelers
               WHEN is_per_person_rate = FALSE AND is_per_night_rate = FALSE THEN hotel_average_nightly_rate/(travel_date_end - travel_date_start)
               WHEN is_per_person_rate = FALSE AND is_per_night_rate = TRUE THEN hotel_average_nightly_rate END as float) as advertiser_price,
               CAST(CASE WHEN is_per_person_rate = TRUE AND is_per_night_rate = FALSE THEN publisher_hotel_price*travelers/(travel_date_end - travel_date_start)
               WHEN is_per_person_rate = TRUE AND is_per_night_rate = TRUE THEN publisher_hotel_price*travelers
               WHEN is_per_person_rate = FALSE AND is_per_night_rate = FALSE THEN publisher_hotel_price/(travel_date_end - travel_date_start) 
               WHEN is_per_person_rate = FALSE AND is_per_night_rate = TRUE THEN publisher_hotel_price END as float) as publisher_price,     
               SUM(CASE WHEN c.fraudulent = TRUE THEN 0 ELSE ISNULL(c.actual_cpc,0) END) as revenue, 
               SUM(CASE WHEN c.fraudulent = FALSE and c.actual_cpc > 0 THEN 1 ELSE 0 END) as clicks, 
               'SERVED' as filter_cause_type, i.base_bid as bid,
               number_of_advertisers_in_auction, positions_filled
          FROM intent_media_log_data_production.ad_calls a
          LEFT OUTER JOIN intent_media_production.hotel_properties hp  ON a.hotel_property_id = hp.id
          INNER JOIN intent_media_log_data_production.impressions i on i.request_id = a.request_id
          LEFT OUTER JOIN Intent_media_log_data_production.clicks c on i.external_id = c.external_impression_id
          WHERE ad_unit_type = 'META' and a.ip_address_blacklisted = false and outcome_type = 'SERVED' 
          and i.ip_address_blacklisted = false  AND a.requested_at_date_in_et = '""" + strmonth + strday + stryear + """'  
          AND i.requested_at_date_in_et = '""" + strmonth + strday + stryear + """'
          AND   hotel_average_nightly_rate  > 0 AND publisher_hotel_price > 0 
          GROUP BY a.request_id, a.ad_unit_id, a.publisher_id,
             a.requested_at_date_in_et, a.requested_at_in_et, advertiser_ID, a.site_currency, auction_position, positions_filled,
                 CASE WHEN hp.country IN ('United States', 'US', 'USA') THEN 'Domestic' 
                      WHEN hp.country is null THEN 'Unknown' 
                      ELSE 'International' END,
               case WHEN a.rank_in_page <= 5 THEN 'TOP 5' ELSE 'NOT TOP 5'END, 
               CAST(CASE WHEN is_per_person_rate = TRUE AND is_per_night_rate = FALSE THEN hotel_average_nightly_rate*travelers/(travel_date_end - travel_date_start)
               WHEN is_per_person_rate = TRUE AND is_per_night_rate = TRUE THEN hotel_average_nightly_rate*travelers
               WHEN is_per_person_rate = FALSE AND is_per_night_rate = FALSE THEN hotel_average_nightly_rate/(travel_date_end - travel_date_start)
               WHEN is_per_person_rate = FALSE AND is_per_night_rate = TRUE THEN hotel_average_nightly_rate END as float),
               CAST(CASE WHEN is_per_person_rate = TRUE AND is_per_night_rate = FALSE THEN publisher_hotel_price*travelers/(travel_date_end - travel_date_start)
               WHEN is_per_person_rate = TRUE AND is_per_night_rate = TRUE THEN publisher_hotel_price*travelers
               WHEN is_per_person_rate = FALSE AND is_per_night_rate = FALSE THEN publisher_hotel_price/(travel_date_end - travel_date_start)
               WHEN is_per_person_rate = FALSE AND is_per_night_rate = TRUE THEN publisher_hotel_price END as float),  
                  i.base_bid, number_of_advertisers_in_auction
                  , travelers, travel_date_end, travel_date_start, rooms, adults, children, a.requested_at_in_et
                  ) a
                  
          UNION
          
          
                  SELECT 0 as Pub_tie, 0 as pub_loss, 0 as pub_win, a.request_id, a.ad_unit_id, 0 as auction_position,
                  travelers, travel_date_end, travel_date_start, rooms, adults, children,  a.requested_at_in_et,
           a.publisher_ID,  a.requested_at_date_in_et, advertiser_ID, a.site_currency, 
                 case WHEN a.rank_in_page <= 5 THEN 'TOP 5' ELSE 'NOT TOP 5'END as rank_in_page,
                 CASE WHEN hp.country IN ('United States', 'US', 'USA') THEN 'Domestic' 
                      WHEN hp.country is null  THEN 'Unknown' 
                      ELSE 'International' END As Dom_Intl,
                  hotel_average_nightly_rate  as advertiser_price, publisher_hotel_price as publisher_price,      
                  0 as revenue, 0 as clicks, filter_cause_type, i.base_bid as bid, number_of_advertisers_in_auction, positions_filled
          FROM intent_media_log_data_production.ad_calls a
          LEFT OUTER JOIN intent_media_production.hotel_properties hp  ON a.hotel_property_id = hp.id
          INNER JOIN intent_media_log_data_production.filtered_advertisements i on i.request_id = a.request_id
          WHERE ad_unit_type = 'META' and a.ip_address_blacklisted = false and outcome_type = 'SERVED' 
          and i.ip_address_blacklisted = false  AND a.requested_at_date_in_et = '""" + strmonth + strday + stryear + """'
          AND i.requested_at_date_in_et = '""" + strmonth + strday + stryear + """'  ; COMMIT 

         ; SELECT analyze_histogram('Intent_media_sandbox_production.EA_ppa_competition_stage_1',100)   ; COMMIT 

                            """
    try:
      subprocess.check_output(['vsql', '-h', host, '-U', user_name, '-w', password, '-c', '\set ON_ERROR_STOP on;', '-c', statement])
    except subprocess.CalledProcessError as e:
      print e.output
      raise Exception("error thrown")
       
def do_analysis2(stryear, strmonth, strday):

    print "infunction_2"
    print strmonth + strday + stryear

    statement = """ 



      DELETE FROM  Intent_media_sandbox_production.EA_ppa_competition_stage_2;  COMMIT; 
      INSERT INTO Intent_media_sandbox_production.EA_ppa_competition_stage_2

      SELECT a.*, Min(advertiser_price) OVER (Partition BY request_ID, filter_cause_type) as winning_Price, 
      case WHEN advertiser_price -  Min(advertiser_price) OVER (Partition BY request_ID, filter_cause_type) < 1 Then 1 ELSE 0 END as win_or_tie,
      case WHEN advertiser_price -  Min(advertiser_price) OVER (Partition BY request_ID, filter_cause_type) < 1 Then 0 ELSE 1 END as loss,
      RANK() OVER (PARTITION BY request_ID, filter_cause_type ORDER BY filter_cause_type, Advertiser) as torder

      FROM 
              (
              
              --------Advertisers
              SELECT pub_win, pub_tie, pub_loss, filter_cause_type, auction_position, request_ID,  a.publisher_id,
              
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
                ad.name as "Ad Unit",  a.ad_unit_id, lp.page_type,  a.requested_at_date_in_et, requested_at_in_et, a.site_currency,  a.rank_in_page, 
                revenue, clicks, bid, a.advertiser_id, Dom_Intl,
                REPLACE(CONCAT(LEFT( e1.ad_display_name,1), LOWER(Right(e1.ad_display_name, length( e1.ad_display_name) -1))),'.com','') as Advertiser, 
                advertiser_price, 0 as is_publisher, number_of_advertisers_in_auction, positions_filled,
                
                ----HARDCODED Publisher Region -- Need to update as new ad_units come online
                CASE WHEN ad.id in (131,130,89,187,184,173,129,206,207, 203, 188, 204) THEN 'US' 
                   WHEN ad.id in (216,147,143,215) THEN 'UK'
                   ELSE 'Unknown' END AS Publisher_Region,
               
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
                else e1.ad_display_name end as Cartel,
                travelers, travel_date_end, travel_date_start, rooms, adults, children
                               
                      
              FROM Intent_media_sandbox_production.EA_ppa_competition_stage_1 a
              INNER JOIN intent_media_production.ad_units ad on ad.id = a.ad_unit_id
              INNER JOIN intent_media_production.sites s on ad.site_ID = s.ID
              INNER JOIN intent_media_production.legal_page_types lp on lp.id = ad.legal_page_type_id
              LEFT JOIN (SELECT id, case WHEN ad_display_name is null then name else ad_display_name END as ad_display_name from intent_media_production.entities) e1 on e1.ID = a.advertiser_ID
              LEFT JOIN (SELECT id, case WHEN ad_display_name is null then name else ad_display_name END as ad_display_name from intent_media_production.entities) e2 on e2.ID = a.publisher_id
              WHERE requested_at_date_in_et = '""" + strmonth + strday + stryear + """'
              
              UNION
              
              --Publishers
              SELECT DISTINCT 0 as pub_win, 0 as pub_tie, 0 as pub_loss, 'SERVED' as filter_cause_type, 0 as auction_position, request_ID,  a.publisher_id,
              
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
              ad.name as "Ad Unit",  a.ad_unit_id, lp.page_type, a.requested_at_date_in_et, requested_at_in_et, a.site_currency,a.rank_in_page, 
              0 as revenue, 0 as clicks, 0 as bid, a.publisher_id, Dom_Intl,
              REPLACE(CONCAT(LEFT(s.display_name,1) , LOWER(Right(s.display_name, length(s.display_name) -1))),'.com','') as Advertiser, 
              publisher_price as advertiser_price, 1 as is_publisher, number_of_advertisers_in_auction, positions_filled,
              
            ----HARDCODED Publisher Region -- Need to update as new ad_units come online
              CASE WHEN ad.id in (131,130,89,187,184,173,129,206,207, 203, 188, 204) THEN 'US' 
                   WHEN ad.id in (216,147,143,215) THEN 'UK'
                   ELSE 'Unknown' END AS Publisher_Region,       
                           
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
              else e2.ad_display_name end as Cartel,
              travelers, travel_date_end, travel_date_start, rooms, adults, children
              
              FROM Intent_media_sandbox_production.EA_ppa_competition_stage_1 a
              INNER JOIN intent_media_production.ad_units ad on ad.id = a.ad_unit_id
              INNER JOIN intent_media_production.sites s on ad.site_ID = s.ID
              INNER JOIN intent_media_production.legal_page_types lp on lp.id = ad.legal_page_type_id
              LEFT JOIN (SELECT id, case WHEN ad_display_name is null then name else ad_display_name END as ad_display_name from intent_media_production.entities) e1 on e1.ID = a.advertiser_ID
              LEFT JOIN (SELECT id, case WHEN ad_display_name is null then name else ad_display_name END as ad_display_name from intent_media_production.entities) e2 on e2.ID = a.publisher_id
              WHERE filter_cause_type = 'SERVED'
              AND requested_at_date_in_et = '""" + strmonth + strday + stryear + """'
              ) a    ; COMMIT 

         ; SELECT analyze_histogram('Intent_media_sandbox_production.EA_ppa_competition_stage_2',100)   ; COMMIT 


                            """

    try:
      subprocess.check_output(['vsql', '-h', host, '-U', user_name, '-w', password, '-c', '\set ON_ERROR_STOP on;', '-c', statement])
    except subprocess.CalledProcessError as e:
      print e.output
      raise Exception("error thrown")



def do_analysis2_advertisers(stryear, strmonth, strday):

    print "infunction_2"
    print strmonth + strday + stryear

    statement = """ 

        DELETE FROM Intent_media_sandbox_production.EA_ppa_competition_advertiser_stage_2;  COMMIT; 
        INSERT INTO Intent_media_sandbox_production.EA_ppa_competition_advertiser_stage_2

        SELECT a.*, Min(advertiser_price) OVER (Partition BY request_ID, filter_cause_type) as winning_Price, 
        case WHEN advertiser_price -  Min(advertiser_price) OVER (Partition BY request_ID, filter_cause_type) < 1 Then 1 ELSE 0 END as win_or_tie,
        case WHEN advertiser_price -  Min(advertiser_price) OVER (Partition BY request_ID, filter_cause_type) < 1 Then 0 ELSE 1 END as loss,
        RANK() OVER (PARTITION BY request_ID, filter_cause_type ORDER BY filter_cause_type, Advertiser) as torder

        FROM 
                (
                
                --------Advertisers
                SELECT pub_tie, filter_cause_type, auction_position, request_ID,  a.publisher_id,
                
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
                  ad.name as "Ad Unit",  a.ad_unit_id, lp.page_type,  a.requested_at_date_in_et, a.site_currency,  a.rank_in_page, 
                  revenue, clicks, bid, a.advertiser_id, Dom_Intl,
                  REPLACE(CONCAT(LEFT( e1.ad_display_name,1), LOWER(Right(e1.ad_display_name, length( e1.ad_display_name) -1))),'.com','') as Advertiser, 
                  advertiser_price, 0 as is_publisher, number_of_advertisers_in_auction, positions_filled,
                  
                  ----HARDCODED Publisher Region -- Need to update as new ad_units come online
                  CASE WHEN ad.id in (131,130,89,187,184,173,129,206,207, 203, 188, 204) THEN 'US' 
                     WHEN ad.id in (216,147,143,215) THEN 'UK'
                     ELSE 'Unknown' END AS Publisher_Region,
                 
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
                                 
                        
                FROM Intent_media_sandbox_production.EA_ppa_competition_stage_1 a
                INNER JOIN intent_media_production.ad_units ad on ad.id = a.ad_unit_id
                INNER JOIN intent_media_production.sites s on ad.site_ID = s.ID
                INNER JOIN intent_media_production.legal_page_types lp on lp.id = ad.legal_page_type_id
                LEFT JOIN (SELECT id, case WHEN ad_display_name is null then name else ad_display_name END as ad_display_name from intent_media_production.entities) e1 on e1.ID = a.advertiser_ID
                LEFT JOIN (SELECT id, case WHEN ad_display_name is null then name else ad_display_name END as ad_display_name from intent_media_production.entities) e2 on e2.ID = a.publisher_id
                
               WHERE requested_at_date_in_et = '""" + strmonth + strday + stryear + """'
                ) a    ; COMMIT 

           ; SELECT analyze_histogram('Intent_media_sandbox_production.EA_ppa_competition_advertiser_stage_2',100)   ; COMMIT 

                            """
    try:
      subprocess.check_output(['vsql', '-h', host, '-U', user_name, '-w', password, '-c', '\set ON_ERROR_STOP on;', '-c', statement])
    except subprocess.CalledProcessError as e:
      print e.output
      raise Exception("error thrown")

def do_analysis3(stryear, strmonth, strday, version):

    print "infunction_3"
    print strmonth + strday + stryear

    statement = """ 

      ---- Create 5 sets of the same data with each Advertiser occupying the first slot once


      DELETE FROM Intent_media_sandbox_production.EA_ppa_competition_""" + version + """stage_3;  COMMIT; 
      INSERT INTO Intent_media_sandbox_production.EA_ppa_competition_""" + version + """stage_3
      

      SELECT b.*, ru.runner_up
      FROM
      (SELECT request_ID, publisher_id, Publisher, site, "Ad Unit", ad_unit_id, page_type, requested_at_date_in_et, site_currency, rank_in_page, 
       revenue, clicks, advertiser_id, Dom_Intl, Advertiser, advertiser_price, is_publisher, Publisher_Region, cartel, winning_price, win_or_tie, loss,
       torder, 1 as meta_rank FROM Intent_media_sandbox_production.EA_ppa_competition_""" + version + """stage_2 a WHERE filter_cause_type = 'SERVED'

      UNION

      SELECT request_ID, publisher_id, Publisher, site, "Ad Unit", ad_unit_id, page_type, requested_at_date_in_et, site_currency, rank_in_page, 
       revenue, clicks, advertiser_id, Dom_Intl, Advertiser, advertiser_price, is_publisher, Publisher_Region, cartel, winning_price, win_or_tie, loss,
      CASE  
      WHEN torder = 2 THEN 1
      WHEN torder < 2 THEN torder + 1
      ELSE torder END as torder, 2 as meta_rank
      FROM Intent_media_sandbox_production.EA_ppa_competition_""" + version + """stage_2
      WHERE filter_cause_type = 'SERVED' AND requested_at_date_in_et = '""" + strmonth + strday + stryear + """'

      UNION

      SELECT request_ID, publisher_id, Publisher, site, "Ad Unit", ad_unit_id, page_type, requested_at_date_in_et, site_currency, rank_in_page, 
       revenue, clicks, advertiser_id, Dom_Intl,  Advertiser, advertiser_price, is_publisher,  Publisher_Region, cartel, winning_price, win_or_tie, loss,
      CASE  
      WHEN torder = 3 THEN 1
      WHEN torder < 3 THEN torder + 1
      ELSE torder END as torder, 3 as meta_rank
      FROM Intent_media_sandbox_production.EA_ppa_competition_""" + version + """stage_2
      WHERE filter_cause_type = 'SERVED' AND requested_at_date_in_et = '""" + strmonth + strday + stryear + """'

      UNION

      SELECT request_ID, publisher_id, Publisher, site, "Ad Unit", ad_unit_id,  page_type, requested_at_date_in_et, site_currency, rank_in_page, 
       revenue, clicks, advertiser_id, Dom_Intl,  Advertiser, advertiser_price, is_publisher,  Publisher_Region, cartel, winning_price, win_or_tie, loss,
      CASE  
      WHEN torder = 4 THEN 1
      WHEN torder < 4 THEN torder + 1
      ELSE torder END as torder, 4 as meta_rank
      FROM Intent_media_sandbox_production.EA_ppa_competition_""" + version + """stage_2
      WHERE filter_cause_type = 'SERVED' AND requested_at_date_in_et = '""" + strmonth + strday + stryear + """'

      UNION

      SELECT request_ID, publisher_id, Publisher, site, "Ad Unit", ad_unit_id,  page_type, requested_at_date_in_et, site_currency, rank_in_page, 
       revenue, clicks, advertiser_id, Dom_Intl, Advertiser, advertiser_price, is_publisher,  Publisher_Region, cartel, winning_price, win_or_tie, loss,
      CASE  
      WHEN torder = 5 THEN 1
      WHEN torder < 5 THEN torder + 1
      ELSE torder END as torder, 5 as meta_rank
      FROM Intent_media_sandbox_production.EA_ppa_competition_""" + version + """stage_2
      WHERE filter_cause_type = 'SERVED' AND requested_at_date_in_et = '""" + strmonth + strday + stryear + """') b
      LEFT OUTER JOIN 
              (SELECT request_id, min(advertiser_price) as runner_up
              FROM Intent_media_sandbox_production.EA_ppa_competition_""" + version + """stage_2
              Where loss = 1 AND  filter_cause_type = 'SERVED'  AND requested_at_date_in_et = '""" + strmonth + strday + stryear + """'
              GROUP BY request_ID) ru
      on ru.request_ID = b.request_ID    ; COMMIT 
 
           ; SELECT analyze_histogram('Intent_media_sandbox_production.EA_ppa_competition_""" + version + """stage_3',100)   ; COMMIT 

                            """
    try:
      subprocess.check_output(['vsql', '-h', host, '-U', user_name, '-w', password, '-c', '\set ON_ERROR_STOP on;', '-c', statement])
    except subprocess.CalledProcessError as e:
      print e.output
      raise Exception("error thrown")

def do_analysis4(stryear, strmonth, strday, version):

    print "infunction_4"
    print strmonth + strday + stryear

    statement = """ 

      ------------ Transpose each set into a single line

      DELETE FROM Intent_media_sandbox_production.EA_ppa_competition_""" + version + """stage_4;  COMMIT; 
      INSERT INTO Intent_media_sandbox_production.EA_ppa_competition_""" + version + """stage_4

      SELECT  a.request_ID, publisher, Publisher_Region, site, publisher_ID, "Ad Unit", ad_unit_id,  page_type, requested_at_date_in_et, site_currency,  rank_in_page,  
              Dom_Intl, is_publisher_1, Advertiser_1, advertiser_id_1, advertiser_price_1, advertiser_1_revenue, advertiser_1_clicks, is_publisher_2, Advertiser_2, advertiser_id_2, 
              advertiser_price_2, advertiser_2_revenue, advertiser_2_clicks, is_publisher_3, Advertiser_3, advertiser_id_3, advertiser_price_3, advertiser_3_revenue, advertiser_3_clicks, is_publisher_4, Advertiser_4, 
              advertiser_id_4, advertiser_price_4, advertiser_4_revenue, advertiser_4_clicks, is_publisher_5, Advertiser_5, advertiser_id_5, advertiser_price_5, advertiser_5_revenue, advertiser_5_clicks,
              cartel_1, cartel_2, cartel_3, cartel_4, cartel_5 , a.winning_price, a.runner_up, win_or_tie_1, isnull(win_or_tie_2,0) + isnull(win_or_tie_3,0) + isnull(win_or_tie_4,0) + isnull(win_or_tie_5,0) as other_win_or_tie, loss_1
      FROM 
              
              (SELECT request_ID, site, Publisher, Publisher_Region, publisher_ID, "Ad Unit", ad_unit_id,  page_type, requested_at_date_in_et, site_currency,  rank_in_page, meta_rank, Dom_Intl, cartel as cartel_1,
              is_publisher as is_publisher_1, Advertiser as Advertiser_1, advertiser_id as advertiser_id_1, advertiser_price as advertiser_price_1, revenue as advertiser_1_revenue, clicks as advertiser_1_clicks,
              win_or_tie as win_or_tie_1, loss as loss_1, winning_price, runner_up
              FROM Intent_media_sandbox_production.EA_ppa_competition_""" + version + """stage_3
              WHERE torder = 1 ) a
              
              LEFT OUTER JOIN
              
              (SELECT request_ID, meta_rank, cartel as cartel_2,
              is_publisher as is_publisher_2, Advertiser as Advertiser_2, advertiser_id as advertiser_id_2, advertiser_price as advertiser_price_2, revenue as advertiser_2_revenue, clicks as advertiser_2_clicks,
              win_or_tie as win_or_tie_2, loss as loss_2
              FROM Intent_media_sandbox_production.EA_ppa_competition_""" + version + """stage_3
              WHERE torder = 2 ) b
              
              ON a.request_ID = b.request_ID AND a.meta_rank = b.meta_rank
              
              LEFT OUTER JOIN
              
              (SELECT request_ID, meta_rank, cartel as cartel_3,
              is_publisher as is_publisher_3, Advertiser as Advertiser_3, advertiser_id as advertiser_id_3, advertiser_price as advertiser_price_3, revenue as advertiser_3_revenue, clicks as advertiser_3_clicks,
              win_or_tie as win_or_tie_3, loss as loss_3
              FROM Intent_media_sandbox_production.EA_ppa_competition_""" + version + """stage_3
              WHERE torder = 3 ) c
              
              ON a.request_ID = c.request_ID AND a.meta_rank = c.meta_rank

              LEFT OUTER JOIN
              
              (SELECT request_ID, meta_rank, cartel as cartel_4,
              is_publisher as is_publisher_4, Advertiser as Advertiser_4, advertiser_id as advertiser_id_4, advertiser_price as advertiser_price_4, revenue as advertiser_4_revenue, clicks as advertiser_4_clicks,
              win_or_tie as win_or_tie_4, loss as loss_4
              FROM Intent_media_sandbox_production.EA_ppa_competition_""" + version + """stage_3
              WHERE torder = 4 ) d
              
              ON a.request_ID = d.request_ID AND a.meta_rank = d.meta_rank

              LEFT OUTER JOIN
              
              (SELECT request_ID, meta_rank, cartel as cartel_5,
              is_publisher as is_publisher_5, Advertiser as Advertiser_5, advertiser_id as advertiser_id_5, advertiser_price as advertiser_price_5, revenue as advertiser_5_revenue, clicks as advertiser_5_clicks,
              win_or_tie as win_or_tie_5, loss as loss_5
              FROM Intent_media_sandbox_production.EA_ppa_competition_""" + version + """stage_3
              WHERE torder = 5 ) e
              
              ON a.request_ID = e.request_ID AND a.meta_rank = e.meta_rank    ; COMMIT 
 
           ; SELECT analyze_histogram('Intent_media_sandbox_production.EA_ppa_competition_""" + version + """stage_4',100)    ; COMMIT 

                            """
    try:
      subprocess.check_output(['vsql', '-h', host, '-U', user_name, '-w', password, '-c', '\set ON_ERROR_STOP on;', '-c', statement])
    except subprocess.CalledProcessError as e:
      print e.output
      raise Exception("error thrown")


def do_analysis5(stryear, strmonth, strday, version):

    print "infunction_5"
    print strmonth + strday + stryear

    statement = """ 


      DELETE FROM intent_media_sandbox_production.EA_ppa_competition_""" + version + """report
      WHERE requested_at_date_in_et in  (SELECT DISTINCT requested_at_date_in_et FROM Intent_media_sandbox_production.EA_ppa_competition_""" + version + """stage_4);  COMMIT; 

      INSERT INTO intent_media_sandbox_production.EA_ppa_competition_report

      SELECT 
              site, Publisher, Publisher_Region, publisher_ID, "Ad Unit", ad_unit_id,  page_type, requested_at_date_in_et, site_currency, rank_in_page, 
              Dom_Intl, is_publisher_1, Advertiser_1, cartel_1, advertiser_id_1, is_publisher_2, Advertiser_2, cartel_2, advertiser_id_2, 
              is_publisher_3, Advertiser_3, cartel_3, advertiser_id_3, is_publisher_4, Advertiser_4,  cartel_4, advertiser_id_4,  is_publisher_5, Advertiser_5,  cartel_5, advertiser_id_5,
              
              COUNT(DISTINCT request_ID) as Ad_Calls,
              
              SUM(CASE WHEN win_or_tie_1 = 1 AND  other_win_or_tie = 0 THEN 1 ELSE 0 END) as WIN,
              SUM(CASE WHEN win_or_tie_1 = 1 AND  other_win_or_tie > 0 THEN 1 ELSE 0 END) as TIE,                                            
              SUM(Loss_1) as LOSS,
              
              CASE WHEN is_publisher_2 = 1 AND advertiser_price_1 - advertiser_price_2 <= -1 THEN 1
               WHEN is_publisher_3 = 1 AND advertiser_price_1 - advertiser_price_3 <= -1 THEN 1
               WHEN is_publisher_4 = 1 AND advertiser_price_1 - advertiser_price_4 <= -1 THEN 1
               WHEN is_publisher_5 = 1 AND advertiser_price_1 - advertiser_price_5 <= -1 THEN 1
              ELSE 0 END as Beats_Publisher,
                     
              SUM(CASE WHEN advertiser_price_1 - isnull(advertiser_price_2,99999999) >= 1 AND  advertiser_price_1 - isnull(advertiser_price_3,99999999) >= 1 AND  advertiser_price_1 - isnull(advertiser_price_4,99999999) >= 1  AND  advertiser_price_1 - isnull(advertiser_price_5,99999999) >= 1 Then 1 ELSE 0 END) AS LOSE_ALL,

              case WHEN advertiser_price_1 - advertiser_price_2 <= -1 Then 1 ELSE 0 END +
              case WHEN advertiser_price_1 - advertiser_price_3 <= -1 Then 1 ELSE 0 END +
              case WHEN advertiser_price_1 - advertiser_price_4 <= -1 Then 1 ELSE 0 END +
              case WHEN advertiser_price_1 - advertiser_price_5 <= -1 Then 1 ELSE 0 END as num_of_wins,
                      
              case WHEN advertiser_price_1 - advertiser_price_2 < 1 AND advertiser_price_1 - advertiser_price_2 > -1 Then 1 ELSE 0 END +
              case WHEN advertiser_price_1 - advertiser_price_3 < 1 AND advertiser_price_1 - advertiser_price_3 > -1 Then 1 ELSE 0 END +       
              case WHEN advertiser_price_1 - advertiser_price_4 < 1 AND advertiser_price_1 - advertiser_price_4 > -1 Then 1 ELSE 0 END +
              case WHEN advertiser_price_1 - advertiser_price_5 < 1 AND advertiser_price_1 - advertiser_price_5 > -1 Then 1 ELSE 0 END as num_of_ties,

              
              case WHEN advertiser_price_1 - advertiser_price_2 >= 1 Then 1 ELSE 0 END +
              case WHEN advertiser_price_1 - advertiser_price_3 >= 1 Then 1 ELSE 0 END +
              case WHEN advertiser_price_1 - advertiser_price_4 >= 1 Then 1 ELSE 0 END +
              case WHEN advertiser_price_1 - advertiser_price_5 >= 1 Then 1 ELSE 0 END as num_of_losses,

              SUM(advertiser_1_revenue) as advertiser_1_revenue,
              SUM(advertiser_2_revenue) as advertiser_2_revenue, 
              SUM(advertiser_3_revenue) as advertiser_3_revenue,
              SUM(advertiser_4_revenue) as advertiser_4_revenue,
              SUM(advertiser_5_revenue) as advertiser_5_revenue,
                                             
              SUM(advertiser_1_clicks) as advertiser_1_clicks,
              SUM(advertiser_2_clicks) as advertiser_2_clicks,
              SUM(advertiser_3_clicks) as advertiser_3_clicks,
              SUM(advertiser_4_clicks) as advertiser_4_clicks,
              SUM(advertiser_5_clicks) as advertiser_5_clicks, 
      ---Win Clicks
              --SUM(CASE WHEN win_or_tie_1 = 1 AND  other_win_or_tie = 0 Then advertiser_1_clicks ELSE 0 END) AS WIN_Clicks_1,
              --SUM(CASE WHEN win_or_tie_1 = 1 AND  other_win_or_tie = 0 Then advertiser_2_clicks ELSE 0 END) AS WIN_Clicks_2,
              --SUM(CASE WHEN win_or_tie_1 = 1 AND  other_win_or_tie = 0 Then advertiser_3_clicks ELSE 0 END) AS WIN_Clicks_3,
              --SUM(CASE WHEN win_or_tie_1 = 1 AND  other_win_or_tie = 0 Then advertiser_4_clicks ELSE 0 END) AS WIN_Clicks_4,
              --SUM(CASE WHEN win_or_tie_1 = 1 AND  other_win_or_tie = 0 Then advertiser_5_clicks ELSE 0 END) AS WIN_Clicks_5,

      --Tie clicks    
              --SUM(CASE WHEN win_or_tie_1 = 1 AND  other_win_or_tie > 0 THEN advertiser_1_clicks ELSE 0 END) as TIE_Clicks_1,                
              --SUM(CASE WHEN win_or_tie_1 = 1 AND  other_win_or_tie > 0 THEN advertiser_2_clicks ELSE 0 END) as TIE_Clicks_2,   
              --SUM(CASE WHEN win_or_tie_1 = 1 AND  other_win_or_tie > 0 THEN advertiser_3_clicks ELSE 0 END) as TIE_Clicks_3,   
              --SUM(CASE WHEN win_or_tie_1 = 1 AND  other_win_or_tie > 0 THEN advertiser_4_clicks ELSE 0 END) as TIE_Clicks_4,   
              --SUM(CASE WHEN win_or_tie_1 = 1 AND  other_win_or_tie > 0 THEN advertiser_5_clicks ELSE 0 END) as TIE_Clicks_5,   
                  
      --Lose Clicks    

              --SUM(CASE WHEN Loss_1 = 1 Then advertiser_1_clicks ELSE 0 END) AS LOSS_Clicks_1,    
              --SUM(CASE WHEN Loss_1 = 1 Then advertiser_2_clicks ELSE 0 END) AS LOSS_Clicks_2,
              --SUM(CASE WHEN Loss_1 = 1 Then advertiser_3_clicks ELSE 0 END) AS LOSS_Clicks_3,        
              --SUM(CASE WHEN Loss_1 = 1 Then advertiser_4_clicks ELSE 0 END) AS LOSS_Clicks_4,        
              --SUM(CASE WHEN Loss_1 = 1 Then advertiser_5_clicks ELSE 0 END) AS LOSS_Clicks_5,
                               
      ---Win Revenue
              --SUM(CASE WHEN win_or_tie_1 = 1 AND  other_win_or_tie = 0 Then advertiser_1_revenue ELSE 0 END) AS WIN_revenue_1,
              --SUM(CASE WHEN win_or_tie_1 = 1 AND  other_win_or_tie = 0 Then advertiser_2_revenue ELSE 0 END) AS WIN_revenue_2,
              --SUM(CASE WHEN win_or_tie_1 = 1 AND  other_win_or_tie = 0 Then advertiser_3_revenue ELSE 0 END) AS WIN_revenue_3,
              --SUM(CASE WHEN win_or_tie_1 = 1 AND  other_win_or_tie = 0 Then advertiser_4_revenue ELSE 0 END) AS WIN_revenue_4,
              --SUM(CASE WHEN win_or_tie_1 = 1 AND  other_win_or_tie = 0 Then advertiser_5_revenue ELSE 0 END) AS WIN_revenue_5,

      --Tie Revenue    
              --SUM(CASE WHEN win_or_tie_1 = 1 AND  other_win_or_tie > 0 THEN advertiser_1_revenue ELSE 0 END) as TIE_revenue_1,                
              --SUM(CASE WHEN win_or_tie_1 = 1 AND  other_win_or_tie > 0 THEN advertiser_2_revenue ELSE 0 END) as TIE_revenue_2,   
              --SUM(CASE WHEN win_or_tie_1 = 1 AND  other_win_or_tie > 0 THEN advertiser_3_revenue ELSE 0 END) as TIE_revenue_3,               
              --SUM(CASE WHEN win_or_tie_1 = 1 AND  other_win_or_tie > 0 THEN advertiser_4_revenue ELSE 0 END) as TIE_revenue_4,             
              --SUM(CASE WHEN win_or_tie_1 = 1 AND  other_win_or_tie > 0 THEN advertiser_5_revenue ELSE 0 END) as TIE_revenue_5,   
                  
      --Lose Revenue    

              --SUM(CASE WHEN Loss_1 = 1 Then advertiser_1_revenue ELSE 0 END) AS LOSS_revenue_1,    
              --SUM(CASE WHEN Loss_1 = 1 Then advertiser_2_revenue ELSE 0 END) AS LOSS_revenue_2,
              --SUM(CASE WHEN Loss_1 = 1 Then advertiser_3_revenue ELSE 0 END) AS LOSS_revenue_3,        
              --SUM(CASE WHEN Loss_1 = 1 Then advertiser_4_revenue ELSE 0 END) AS LOSS_revenue_4,        
              --SUM(CASE WHEN Loss_1 = 1 Then advertiser_5_revenue ELSE 0 END) AS LOSS_revenue_5,
              
              
              CASE WHEN cartel_1 = cartel_2 THEN 1 ELSE 0 END + CASE WHEN cartel_1 = cartel_3 THEN 1 ELSE 0 END + CASE WHEN cartel_1 = cartel_4 THEN 1 ELSE 0 END + CASE WHEN cartel_1 = cartel_5 THEN 1 ELSE 0 END as cartel_count,

      ---- PCT Difference to winning price of loss
              SUM(CASE WHEN Loss_1 = 1 THEN  (advertiser_price_1 - winning_price)/winning_price ELSE 0 END) as SUM_LOSS_PRCT_Differences,
              --SUM(CASE WHEN Loss_1 = 1 THEN ((advertiser_price_1 - winning_price)/winning_price)^2 ELSE 0 END) as SUM_LOSS_PRCT_Differences_sqr,
              
      ---- PCT Difference to next highest price of win -- use Runner_up_price

              SUM(CASE WHEN win_or_tie_1 = 1 AND  other_win_or_tie = 0 THEN  (advertiser_price_1 - isnull(runner_up, advertiser_price_1))/runner_up ELSE 0 END) as SUM_WIN_PRCT_Differences,
              --SUM(CASE WHEN win_or_tie_1 = 1 AND  other_win_or_tie = 0 THEN ((advertiser_price_1 - isnull(runner_up, advertiser_price_1))/runner_up)^2 ELSE 0 END) as SUM_WIN_PRCT_Differences_sqr,

                                             
              SUM((advertiser_price_1 - advertiser_price_2)/advertiser_price_2) as SUM_PRCT_Differences_2,
              SUM((advertiser_price_1 - advertiser_price_3)/advertiser_price_3) as SUM_PRCT_Differences_3,
              SUM((advertiser_price_1 - advertiser_price_4)/advertiser_price_4) as SUM_PRCT_Differences_4,
              SUM((advertiser_price_1 - advertiser_price_5)/advertiser_price_5) as SUM_PRCT_Differences_5,


      ---- PCT Differences over 50pct are used to show how often something goes really wrong
              SUM(CASE WHEN  (advertiser_price_1 - advertiser_price_2)/advertiser_price_2 > .5 THEN 1 ELSE 0 END)  +
              SUM(CASE WHEN  (advertiser_price_1 - advertiser_price_3)/advertiser_price_3 > .5 THEN 1 ELSE 0 END)  +
              SUM(CASE WHEN  (advertiser_price_1 - advertiser_price_4)/advertiser_price_4 > .5 THEN 1 ELSE 0 END)  +
              SUM(CASE WHEN  (advertiser_price_1 - advertiser_price_5)/advertiser_price_5 > .5 THEN 1 ELSE 0 END)  as PRCT_Differences_Over_50,
                      
              SUM(CASE WHEN  (advertiser_price_1 - advertiser_price_2)/advertiser_price_2 < -.5 THEN 1 ELSE 0 END) +
              SUM(CASE WHEN  (advertiser_price_1 - advertiser_price_3)/advertiser_price_3 < -.5 THEN 1 ELSE 0 END) +
              SUM(CASE WHEN  (advertiser_price_1 - advertiser_price_4)/advertiser_price_4 < -.5 THEN 1 ELSE 0 END) +
              SUM(CASE WHEN  (advertiser_price_1 - advertiser_price_5)/advertiser_price_5 < -.5 THEN 1 ELSE 0 END)  as PRCT_Differences_Under_neg50,
       
       ---- Clicks of PCT Differences over 50%  - Used with Publisher
              SUM(CASE WHEN  (advertiser_price_1 - advertiser_price_2)/advertiser_price_2 > .5 THEN advertiser_2_clicks ELSE 0 END)  +
              SUM(CASE WHEN  (advertiser_price_1 - advertiser_price_3)/advertiser_price_3 > .5 THEN advertiser_3_clicks ELSE 0 END)  +
              SUM(CASE WHEN  (advertiser_price_1 - advertiser_price_4)/advertiser_price_4 > .5 THEN advertiser_4_clicks ELSE 0 END)  +
              SUM(CASE WHEN  (advertiser_price_1 - advertiser_price_5)/advertiser_price_5 > .5 THEN advertiser_5_clicks ELSE 0 END)  as Clicks_PRCT_Differences_Over_50,
                      
              SUM(CASE WHEN  (advertiser_price_1 - advertiser_price_2)/advertiser_price_2 < -.5 THEN advertiser_2_clicks ELSE 0 END) +
              SUM(CASE WHEN  (advertiser_price_1 - advertiser_price_3)/advertiser_price_3 < -.5 THEN advertiser_3_clicks ELSE 0 END) +
              SUM(CASE WHEN  (advertiser_price_1 - advertiser_price_4)/advertiser_price_4 < -.5 THEN advertiser_4_clicks ELSE 0 END) +
              SUM(CASE WHEN  (advertiser_price_1 - advertiser_price_5)/advertiser_price_5 < -.5 THEN advertiser_5_clicks ELSE 0 END)  as Clicks_PRCT_Differences_Under_neg50,        

      -----   clicks that are more then 50pct price differences from runner_up - Used with Advertiser WINS
              SUM(CASE WHEN  (advertiser_price_1 - runner_up)/runner_up < -.5 THEN advertiser_1_clicks ELSE 0 END)  as advertiser_1_clicks_50_runner_up,
              

              
              SUM(case WHEN advertiser_price_1 - advertiser_price_2 <= -1 Then 1 ELSE 0 END) as WINS_1_2,
              SUM(case WHEN advertiser_price_1 - advertiser_price_3 <= -1 Then 1 ELSE 0 END) as WINS_1_3,
              SUM(case WHEN advertiser_price_1 - advertiser_price_4 <= -1 Then 1 ELSE 0 END) as WINS_1_4 ,       
              SUM(case WHEN advertiser_price_1 - advertiser_price_5 <= -1 Then 1 ELSE 0 END) as WINS_1_5,
                      
              SUM(case WHEN advertiser_price_1 - advertiser_price_2 < 1 AND advertiser_price_1 - advertiser_price_2 > -1 Then 1 ELSE 0 END) as TIES_1_2,
              SUM(case WHEN advertiser_price_1 - advertiser_price_3 < 1 AND advertiser_price_1 - advertiser_price_3 > -1 Then 1 ELSE 0 END) as TIES_1_3,        
              SUM(case WHEN advertiser_price_1 - advertiser_price_4 < 1 AND advertiser_price_1 - advertiser_price_4 > -1 Then 1 ELSE 0 END) as TIES_1_4,
              SUM(case WHEN advertiser_price_1 - advertiser_price_5 < 1 AND advertiser_price_1 - advertiser_price_5 > -1 Then 1 ELSE 0 END) as TIES_1_5,

              
              SUM(case WHEN advertiser_price_1 - advertiser_price_2 >= 1 Then 1 ELSE 0 END) as LOSSES_1_2,
              SUM(case WHEN advertiser_price_1 - advertiser_price_3 >= 1 Then 1 ELSE 0 END) as LOSSES_1_3,        
              SUM(case WHEN advertiser_price_1 - advertiser_price_4 >= 1 Then 1 ELSE 0 END) as LOSSES_1_4,
              SUM(case WHEN advertiser_price_1 - advertiser_price_5 >= 1 Then 1 ELSE 0 END) as LOSSES_1_5,
                               
      --Prct Diff        
              SUM(case WHEN advertiser_price_1 - advertiser_price_2 <= -1 Then (advertiser_price_1 - advertiser_price_2)/advertiser_price_2 ELSE null END) as WINS_SUM_PRCT_Differences_1_2,
              SUM(case WHEN advertiser_price_1 - advertiser_price_3 <= -1 Then (advertiser_price_1 - advertiser_price_3)/advertiser_price_3 ELSE null END) as WINS_SUM_PRCT_Differences_1_3,        
              SUM(case WHEN advertiser_price_1 - advertiser_price_4 <= -1 Then (advertiser_price_1 - advertiser_price_4)/advertiser_price_4 ELSE null END) as WINS_SUM_PRCT_Differences_1_4,
              SUM(case WHEN advertiser_price_1 - advertiser_price_5 <= -1 Then (advertiser_price_1 - advertiser_price_5)/advertiser_price_5 ELSE null END) as WINS_SUM_PRCT_Differences_1_5,                
      --Prct Diff^2
 
      --Prct Diff        
              SUM(case WHEN advertiser_price_1 - advertiser_price_2 >= 1 Then (advertiser_price_1 - advertiser_price_2)/advertiser_price_2 ELSE null END) as LOSES_SUM_PRCT_Differences_1_2,
              SUM(case WHEN advertiser_price_1 - advertiser_price_3 >= 1 Then (advertiser_price_1 - advertiser_price_3)/advertiser_price_3 ELSE null END) as LOSES_SUM_PRCT_Differences_1_3,        
              SUM(case WHEN advertiser_price_1 - advertiser_price_4 >= 1 Then (advertiser_price_1 - advertiser_price_4)/advertiser_price_4 ELSE null END) as LOSES_SUM_PRCT_Differences_1_4,
              SUM(case WHEN advertiser_price_1 - advertiser_price_5 >= 1 Then (advertiser_price_1 - advertiser_price_5)/advertiser_price_5 ELSE null END) as LOSES_SUM_PRCT_Differences_1_5                    

      FROM Intent_media_sandbox_production.EA_ppa_competition_""" + version + """stage_4
      GROUP BY 
              site, Publisher, Publisher_Region, publisher_ID, "Ad Unit", ad_unit_id,  page_type, requested_at_date_in_et, site_currency, rank_in_page, Dom_Intl,
               is_publisher_1, Advertiser_1, advertiser_id_1, is_publisher_2, Advertiser_2, advertiser_id_2, 
              is_publisher_3, Advertiser_3, advertiser_id_3, is_publisher_4, Advertiser_4, advertiser_id_4,  is_publisher_5, Advertiser_5, advertiser_id_5,
              cartel_1, cartel_2, cartel_3, cartel_4, cartel_5,
              
              case WHEN advertiser_price_1 - advertiser_price_2 <= -1 Then 1 ELSE 0 END +
              case WHEN advertiser_price_1 - advertiser_price_3 <= -1 Then 1 ELSE 0 END +
              case WHEN advertiser_price_1 - advertiser_price_4 <= -1 Then 1 ELSE 0 END +
              case WHEN advertiser_price_1 - advertiser_price_5 <= -1 Then 1 ELSE 0 END ,
                      
              case WHEN advertiser_price_1 - advertiser_price_2 < 1 AND advertiser_price_1 - advertiser_price_2 > -1 Then 1 ELSE 0 END +
              case WHEN advertiser_price_1 - advertiser_price_3 < 1 AND advertiser_price_1 - advertiser_price_3 > -1 Then 1 ELSE 0 END +       
              case WHEN advertiser_price_1 - advertiser_price_4 < 1 AND advertiser_price_1 - advertiser_price_4 > -1 Then 1 ELSE 0 END +
              case WHEN advertiser_price_1 - advertiser_price_5 < 1 AND advertiser_price_1 - advertiser_price_5 > -1 Then 1 ELSE 0 END ,

              
              case WHEN advertiser_price_1 - advertiser_price_2 >= 1 Then 1 ELSE 0 END +
              case WHEN advertiser_price_1 - advertiser_price_3 >= 1 Then 1 ELSE 0 END +
              case WHEN advertiser_price_1 - advertiser_price_4 >= 1 Then 1 ELSE 0 END +
              case WHEN advertiser_price_1 - advertiser_price_5 >= 1 Then 1 ELSE 0 END,
              
              
              CASE WHEN is_publisher_2 = 1 AND advertiser_price_1 - advertiser_price_2 <= -1 THEN 1
               WHEN is_publisher_3 = 1 AND advertiser_price_1 - advertiser_price_3 <= -1 THEN 1
               WHEN is_publisher_4 = 1 AND advertiser_price_1 - advertiser_price_4 <= -1 THEN 1
               WHEN is_publisher_5 = 1 AND advertiser_price_1 - advertiser_price_5 <= -1 THEN 1
              ELSE 0 END     ; COMMIT 

           ; SELECT analyze_histogram('Intent_media_sandbox_production.EA_ppa_competition_""" + version + """report',100)     ; COMMIT 

                            """
    try:
      subprocess.check_output(['vsql', '-h', host, '-U', user_name, '-w', password, '-c', '\set ON_ERROR_STOP on;', '-c', statement])
    except subprocess.CalledProcessError as e:
      print e.output
      raise Exception("error thrown")

def do_analysis6(stryear, strmonth, strday):

# _"""  + strmonth + strday + stryear + """

    print "infunction_6"
    print strmonth + strday + stryear

    statement = """ 

        DELETE FROM intent_media_sandbox_production.EA_ppa_auction_reporting
        WHERE requested_at_date_in_et in  (SELECT DISTINCT requested_at_date_in_et FROM Intent_media_sandbox_production.EA_ppa_competition_stage_2);  COMMIT; 


        INSERT INTO  Intent_media_sandbox_production.EA_ppa_auction_reporting
        SELECT   count(DISTINCT a1.request_id) as ad_calls,  Pub_win, Pub_Tie, Pub_loss, Publisher, publisher_region, Publisher_id, Site, "Ad Unit", ad_unit_id, page_type, requested_at_date_in_et, 
                 site_currency, rank_in_Page, Dom_Intl, number_of_advertisers_in_auction, positions_filled,
                 SUM(clicks_1) as clicks_1, SUM(revenue_1) as revenue_1, SUM(clicks_2) as clicks_2, SUM(revenue_2) as revenue_2, SUM(clicks_3) as clicks_3, 
                 SUM(revenue_3) as revenue_3, SUM(clicks_4) as clicks_4, SUM(revenue_4) as revenue_4, SUM(bid_1) as bid_1, SUM(bid_2) as bid_2, SUM(bid_3) as bid_3, SUM(bid_4) as bid_4,
                 SUM(Num_Bids_At_Minimum) as Num_Bids_At_Minimum, SUM(Clicks_At_Minimum) as Clicks_At_Minimum, SUM(Unserved_Impressions) as Unserved_Impressions

        FROM 
                ---Position 1 plus Ad Call Info
                (SELECT request_id, Pub_win, Pub_Tie, Pub_loss, Publisher, publisher_region, Publisher_id, Site, "Ad Unit", ad_unit_id, page_type, requested_at_date_in_et, site_currency, rank_in_Page, Dom_Intl, 
                number_of_advertisers_in_auction, positions_filled,
                clicks as clicks_1, revenue as revenue_1, bid as bid_1
                FROM Intent_media_sandbox_production.EA_ppa_competition_stage_2
                WHERE auction_position = 1 ) a1

        LEFT JOIN  
                --Position 2
                (SELECT request_id,  clicks as clicks_2, revenue as revenue_2, bid as bid_2
                FROM Intent_media_sandbox_production.EA_ppa_competition_stage_2
                WHERE auction_position = 2) a2 ON a2.request_id = a1.request_id

        LEFT JOIN  
                --Position 3
                (SELECT request_id,  clicks as clicks_3, revenue as revenue_3, bid as bid_3
                FROM Intent_media_sandbox_production.EA_ppa_competition_stage_2
                WHERE auction_position = 3) a3 ON a3.request_id = a1.request_id

        LEFT JOIN  
                --Position 4
                (SELECT request_id,  clicks as clicks_4, revenue as revenue_4, bid as bid_4
                FROM Intent_media_sandbox_production.EA_ppa_competition_stage_2
                WHERE auction_position = 4) a4 ON a4.request_id = a1.request_id

        LEFT JOIN 
                -- Number of Bids at Min
                (SELECT request_id, COUNT(*) as Num_Bids_At_Minimum
                FROM Intent_media_sandbox_production.EA_ppa_competition_stage_2
                WHERE bid = 1.75 AND auction_position > 0
                GROUP BY request_id) minb ON a1.request_id = minb.request_id

        LEFT JOIN 
                -- Number of clicks at Min
                (SELECT request_id, SUM(clicks) as Clicks_At_Minimum
                FROM Intent_media_sandbox_production.EA_ppa_competition_stage_2
                WHERE BID = 1.75 AND auction_position > 0 AND revenue > 0
                GROUP BY request_id) minclick ON a1.request_id = minclick.request_id

        LEFT JOIN 
                -- Count of unserved impressions
                (SELECT request_id, COUNT(*) as Unserved_Impressions
                FROM Intent_media_sandbox_production.EA_ppa_competition_stage_2
                WHERE filter_cause_type = 'BID_TOO_LOW'
                GROUP BY request_id) f ON a1.request_id = f.request_id

        GROUP BY   Pub_win, Pub_Tie, Pub_loss, Publisher, Publisher_id, publisher_region, Site, "Ad Unit", ad_unit_id, page_type, requested_at_date_in_et, site_currency, rank_in_Page, Dom_Intl, number_of_advertisers_in_auction, positions_filled   ; COMMIT 

           ; SELECT analyze_histogram('intent_media_sandbox_production.EA_ppa_auction_reporting',100)    ; COMMIT 

                            """
    try:
      subprocess.check_output(['vsql', '-h', host, '-U', user_name, '-w', password, '-c', '\set ON_ERROR_STOP on;', '-c', statement])
    except subprocess.CalledProcessError as e:
      print e.output
      raise Exception("error thrown")


def runtheloop(year, month, day, step):

    stryear = str(year)

    # Add zeros for single digit days and forward slashes for dates
    if len(str(day)) == 1:
        strday = '0' + str(day) + '/'
    else:
        strday = str(day) + '/'

    if len(str(month)) == 1:
        strmonth = '0' + str(month) + '/'
    else:
        strmonth = str(month) + '/'

    daystorun = (datetime.date.today() - datetime.date(year,month,day)).days - 1
    ongoing_date = datetime.date(year,month,day)
    print daystorun

    while daystorun >= 0:
        
        # Redfine string variables as the loop progresses
        if len(str(ongoing_date.day)) == 1:
            strday = '0' + str(ongoing_date.day) + '/'
        else:
            strday = str(ongoing_date.day) + '/'

        if len(str(ongoing_date.month)) == 1:
            strmonth = '0' + str(ongoing_date.month) + '/'
        else:
            strmonth = str(ongoing_date.month) + '/'

        stryear = str(ongoing_date.year)


        try:
            print "starting"

            # Run each of the functions for each part of the sql query.  I do it in steps so the loop can pick back up in the same place in case of an error
            if step == 1:
                do_analysis1(stryear, strmonth, strday);
                step = 2;

            if step == 2:                
                do_analysis2(stryear, strmonth, strday)
                step = 3

            if step == 3:                
                do_analysis2_advertisers(stryear, strmonth, strday)
                step = 4                

            if step == 4:
                do_analysis3(stryear, strmonth, strday, '')            
                step = 5

            if step == 5:
                do_analysis3(stryear, strmonth, strday, 'advertiser_')            
                step = 6

            if step == 6:
                do_analysis4(stryear, strmonth, strday, '')            
                step = 7

            if step == 7:
                do_analysis4(stryear, strmonth, strday, 'advertiser_')            
                step = 8

            if step == 8:
                do_analysis5(stryear, strmonth[:-1], strday[:-1], '')
                step = 9   

            if step == 9:
                do_analysis5(stryear, strmonth[:-1], strday[:-1], 'advertiser_')
                step = 10

            if step == 10:
                do_analysis6(stryear, strmonth[:-1], strday[:-1])
                step = 1                    

            print "ending"
            daystorun = daystorun - 1
            day = day + 1
            ongoing_date = ongoing_date + datetime.timedelta(days=1)

        except Exception:
            print "Error"
            print "Error", sys.exc_info()[0]
            runtheloop(ongoing_date.year, ongoing_date.month, ongoing_date.day, step)

runtheloop(2015, 3, 1, 1)