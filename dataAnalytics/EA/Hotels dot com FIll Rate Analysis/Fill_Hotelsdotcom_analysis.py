
import numpy as np
import pandas as pd
import vertica_python as vp
import datetime
import sys


def create_connection():
    conn = vp.connect({
        'host': 'production-vertica-cluster-with-failover.internal.intentmedia.net',
        'port': 5433,
        'user': 'Tableau',
        'password': '9WOGfffN',
        'database': 'intent_media',
        'option': 'ConnectionLoadBalance=1; autocommit = true'
    })

    return conn

 # to_date('""" + strmonth + strday + stryear + """','mm/dd/yyyy')

def do_analysis1(stryear, strmonth, strday):

    print "infunction_1"
    print strmonth + strday + stryear

    conn = create_connection()
    cur = conn.cursor()
    sql_string = """ 

        DROP TABLE IF EXISTS  Intent_media_sandbox_production.EA_ppa_hotels_fill_rate1;
        CREATE TABLE Intent_media_sandbox_production.EA_ppa_hotels_fill_rate1
         AS

        SELECT a.request_id, a.ad_unit_id, a.requested_at_date_in_et, i.advertiser_ID, a.site_currency, number_of_advertisers_in_auction,  i.ip_address_blacklisted , outcome_type, a.publisher_id, 'served' as filter_cause_type
        FROM intent_media_log_data_production.ad_calls a
        LEFT OUTER JOIN intent_media_log_data_production.impressions i on i.request_id = a.request_id
        WHERE ad_unit_type = 'META' and a.ip_address_blacklisted = false AND a.Publisher_ID = 109
        AND a.requested_at_date_in_et = to_date('""" + strmonth + strday + stryear + """','mm/dd/yyyy')
        
        UNION

        SELECT a.request_id, a.ad_unit_id, a.requested_at_date_in_et, i.advertiser_ID, a.site_currency, number_of_advertisers_in_auction,  i.ip_address_blacklisted , outcome_type, a.publisher_id, filter_cause_type
        FROM intent_media_log_data_production.ad_calls a
        LEFT OUTER JOIN intent_media_log_data_production.filtered_advertisements i on i.request_id = a.request_id
        WHERE ad_unit_type = 'META' and a.ip_address_blacklisted = false AND a.Publisher_ID = 109
        AND a.requested_at_date_in_et = to_date('""" + strmonth + strday + stryear + """','mm/dd/yyyy')      

                            """
    cur.execute(sql_string)
    conn.close()
       
def do_analysis2(stryear, strmonth, strday):

    print "infunction_2"
    print strmonth + strday + stryear

    conn = create_connection()
    cur = conn.cursor()
    sql_string = """ 


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

                            """
    cur.execute(sql_string)
    conn.close()


def do_analysis5(stryear, strmonth, strday):

# _"""  + strmonth + strday + stryear + """

    print "infunction_5"
    print strmonth + strday + stryear

    conn = create_connection()
    cur = conn.cursor()
    sql_string = """ 



      DROP TABLE IF EXISTS Intent_media_sandbox_production.EA_ppa_hotels_fill_rate_"""  + strmonth + strday + stryear + """;
      CREATE TABLE Intent_media_sandbox_production.EA_ppa_hotels_fill_rate_"""  + strmonth + strday + stryear + """
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

                            """
    
    cur.execute(sql_string)
    conn.close()

def runtheloop(year, month, day, step):

    stryear = str(year)

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


            if step == 1:
                do_analysis1(stryear, strmonth, strday);
                step = 2;

            if step == 2:                
                do_analysis2(stryear, strmonth, strday)
                step = 5

            if step == 5:
                do_analysis5(stryear, strmonth[:-1], strday[:-1])
                step = 1    

            print "ending"
            daystorun = daystorun - 1
            day = day + 1
            ongoing_date = ongoing_date + datetime.timedelta(days=1)

        except Exception:
            print "Error"
            print "Error", sys.exc_info()[0]
            runtheloop(ongoing_date.year, ongoing_date.month, ongoing_date.day, step)

runtheloop(2014, 12, 1, 1)