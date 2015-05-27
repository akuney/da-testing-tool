/* Table 1 - Ad Calls */
drop table if exists intent_media_sandbox_production.SPYB_MVT_1818_ac;
create table intent_media_sandbox_production.SPYB_MVT_1818_ac as
    select 
        site_type,
        product_category_type,
        publisher_user_id,
        webuser_id,
        case 
            when multivariate_test_attributes like '%"QUALITY_SCORE_MODEL":"old_model"%' then 'old_model'
            when multivariate_test_attributes like '%"QUALITY_SCORE_MODEL":"new_model"%' then 'new_model'
            else 'not_found' end as mvt_value_1, 
        request_id,
        requested_at
        from intent_media_log_data_production.ad_calls
        where requested_at_date_in_et between '2014-10-01' and '2014-10-04'
            and ip_address_blacklisted = 0
/*            and site_id in (2, 3, 4, 6, 8, 9, 12, 13, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30,
	                       31, 32, 33, 34, 35, 42, 43, 45)*/
	    and site_type in ('AIRFASTTICKETS', 'AIRTKT', 'BOOKIT', 'BUDGETAIR', 'CHEAPAIR', 'CHEAPOAIR', 'CHEAPTICKETS', 
	                       'EBOOKERS', 'EXPEDIA', 'FARESPOTTER', 'FLY_DOT_COM', 'GOGOBOT', 'HIPMUNK', 'HOTWIRE', 
	                       'HOTWIRE_MEDIA_FILL_IN', 'HOTWIRE_UK', 'KAYAK', 'KAYAK_UK', 'LASTMINUTE_DOT_COM', 'LOWCOSTAIRLINES', 
	                       'LOWFARES', 'ONETRAVEL', 'ORBITZ_GLOBAL', 'TRAVELZOO', 'TRIPADVISOR', 'TRIPDOTCOM', 'TVLY', 'VAYAMA', 'WEBJET')
            and ad_unit_type = 'CT'
            and product_category_type = 'FLIGHTS'
            and outcome_type = 'SERVED'
            and multivariate_version_id >= 1818;
  

/* Table 2 - Reference Table - advertisements, ad_groups, campaigns */
drop table if exists intent_media_sandbox_production.SPYB_MVT_1818_ad;
create table intent_media_sandbox_production.SPYB_MVT_1818_ad as
    select
        ad.id as advertisement_id,
        c.advertiser_id
    from intent_media_sandbox_production.SP_advertisements_copy ad
    inner join intent_media_production.ad_groups ag on ad.ad_group_id = ag.id
    inner join intent_media_production.campaigns c on c.id = ag.campaign_id;

/* Table 3 - Clicks */
drop table if exists intent_media_sandbox_production.SPYB_MVT_1818_c;
create table intent_media_sandbox_production.SPYB_MVT_1818_c as
    select
        c.site_type, 
        c.request_id,
        c.requested_at,
        c.ad_call_request_id,
        c.publisher_user_id,
        c.webuser_id,
        c.actual_cpc,
        ad.advertiser_id
    from intent_media_log_data_production.clicks c
    inner join intent_media_log_data_production.ad_calls ac
        on c.ad_call_request_id = ac.request_id
    inner join intent_media_sandbox_production.SPYB_MVT_1818_ad ad
        on c.advertisement_id = ad.advertisement_id
    where c.requested_at_date_in_et between '2014-10-01' and (date('2014-10-04') + interval '1 day')
        and ac.requested_at_date_in_et between '2014-10-01' and '2014-10-04'
        and ac.ip_address_blacklisted = 0
        and c.ip_address_blacklisted = 0
        and c.fraudulent = 0
        and c.site_type in ('AIRFASTTICKETS', 'AIRTKT', 'BOOKIT', 'BUDGETAIR', 'CHEAPAIR', 'CHEAPOAIR', 'CHEAPTICKETS', 
	                       'EBOOKERS', 'EXPEDIA', 'FARESPOTTER', 'FLY_DOT_COM', 'GOGOBOT', 'HIPMUNK', 'HOTWIRE', 
	                       'HOTWIRE_MEDIA_FILL_IN', 'HOTWIRE_UK', 'KAYAK', 'KAYAK_UK', 'LASTMINUTE_DOT_COM', 'LOWCOSTAIRLINES', 
	                       'LOWFARES', 'ONETRAVEL', 'ORBITZ_GLOBAL', 'TRAVELZOO', 'TRIPADVISOR', 'TRIPDOTCOM', 'TVLY', 'VAYAMA', 'WEBJET')
        and ac.site_type in ('AIRFASTTICKETS', 'AIRTKT', 'BOOKIT', 'BUDGETAIR', 'CHEAPAIR', 'CHEAPOAIR', 'CHEAPTICKETS', 
	                       'EBOOKERS', 'EXPEDIA', 'FARESPOTTER', 'FLY_DOT_COM', 'GOGOBOT', 'HIPMUNK', 'HOTWIRE', 
	                       'HOTWIRE_MEDIA_FILL_IN', 'HOTWIRE_UK', 'KAYAK', 'KAYAK_UK', 'LASTMINUTE_DOT_COM', 'LOWCOSTAIRLINES', 
	                       'LOWFARES', 'ONETRAVEL', 'ORBITZ_GLOBAL', 'TRAVELZOO', 'TRIPADVISOR', 'TRIPDOTCOM', 'TVLY', 'VAYAMA', 'WEBJET')
        and ac.outcome_type = 'SERVED'
        and ac.ad_unit_type = 'CT'
        and ac.product_category_type = 'FLIGHTS';

/* Table 4 - Deduped Conversions */
drop table if exists intent_media_sandbox_production.SPYB_MVT_1818_con;
create table intent_media_sandbox_production.SPYB_MVT_1818_con as
    select 
        con.request_id,
        con.requested_at,
        con.site_type,
        con.entity_id,
        con.order_id,
        con.publisher_user_id,
        con.webuser_id,
        con.conversion_value,
        con.net_conversion_value
    from intent_media_log_data_production.conversions con
    inner join 
        (select 
            entity_id,
            order_id,
            min(requested_at) as min_requested_at
        from intent_media_log_data_production.conversions
        where requested_at_date_in_et between '2014-10-01' and (date('2014-10-04') + interval '31 day')
            and ip_address_blacklisted = 0
            and site_type in ('AIRFASTTICKETS', 'AIRTKT', 'BOOKIT', 'BUDGETAIR', 'CHEAPAIR', 'CHEAPOAIR', 'CHEAPTICKETS', 
	                       'EBOOKERS', 'EXPEDIA', 'FARESPOTTER', 'FLY_DOT_COM', 'GOGOBOT', 'HIPMUNK', 'HOTWIRE', 
	                       'HOTWIRE_MEDIA_FILL_IN', 'HOTWIRE_UK', 'KAYAK', 'KAYAK_UK', 'LASTMINUTE_DOT_COM', 'LOWCOSTAIRLINES', 
	                       'LOWFARES', 'ONETRAVEL', 'ORBITZ_GLOBAL', 'TRAVELZOO', 'TRIPADVISOR', 'TRIPDOTCOM', 'TVLY', 'VAYAMA', 'WEBJET') /* Instead of entity_id */
            and entity_id in (59528, 59777, 61224, 62118, 66539, 70994, 87697, 106574, 152665)
            and order_id is not null
        group by 
            entity_id,
            order_id) e_o_min 
    on con.entity_id = e_o_min.entity_id 
    and con.order_id = e_o_min.order_id 
    and con.requested_at = e_o_min.min_requested_at
    
    union
    
    select 
        request_id,
        requested_at,
        site_type,
        entity_id,
        order_id, 
        publisher_user_id,
        webuser_id,
        conversion_value,
        net_conversion_value
    from intent_media_log_data_production.conversions
    where requested_at_date_in_et between '2014-10-01' and (date('2014-10-04') + interval '31 day')
        and ip_address_blacklisted = 0
        and site_type in ('AIRFASTTICKETS', 'AIRTKT', 'BOOKIT', 'BUDGETAIR', 'CHEAPAIR', 'CHEAPOAIR', 'CHEAPTICKETS', 
	                       'EBOOKERS', 'EXPEDIA', 'FARESPOTTER', 'FLY_DOT_COM', 'GOGOBOT', 'HIPMUNK', 'HOTWIRE', 
	                       'HOTWIRE_MEDIA_FILL_IN', 'HOTWIRE_UK', 'KAYAK', 'KAYAK_UK', 'LASTMINUTE_DOT_COM', 'LOWCOSTAIRLINES', 
	                       'LOWFARES', 'ONETRAVEL', 'ORBITZ_GLOBAL', 'TRAVELZOO', 'TRIPADVISOR', 'TRIPDOTCOM', 'TVLY', 'VAYAMA', 'WEBJET') /* Instead of entity_id */
        and entity_id in (59528, 59777, 61224, 62118, 66539, 70994, 87697, 106574, 152665)
        and order_id is null;

/* Table 6 - Reference Table for the time difference between Clicks and Conversions */
drop table if exists intent_media_sandbox_production.SPYB_MVT_1818_con_c;
create table intent_media_sandbox_production.SPYB_MVT_1818_con_c as
        select
            con.request_id as con_request_id,
            c.request_id as c_request_id,
            con.requested_at as con_requested_at,
            c.requested_at as c_requested_at,
            datediff('ss', c.requested_at, con.requested_at) as con_c_time_diff
        from intent_media_sandbox_production.SPYB_MVT_1818_con con
        inner join intent_media_sandbox_production.SPYB_MVT_1818_c c 
            on con.webuser_id = c.webuser_id
            and con.entity_id = c.advertiser_id;

/* Table 7 - Attributed Conversion with Clicks (30 days) */
drop table if exists intent_media_sandbox_production.SPYB_MVT_1818_c_con;
create table intent_media_sandbox_production.SPYB_MVT_1818_c_con as
    select
        c.site_type,
        c.advertiser_id,
        c.ad_call_request_id,
        c.publisher_user_id,
        c.webuser_id,
        c.request_id,
        c.requested_at,
        c.actual_cpc,
        case when att_con.conversions is null then 0 else att_con.conversions end as clicked_conversions
    from intent_media_sandbox_production.SPYB_MVT_1818_c c
    left join 
        (select
            con_c.c_request_id,
            count(con_c.con_request_id) as conversions
        from intent_media_sandbox_production.SPYB_MVT_1818_con_c con_c
        inner join 
            (select 
                con_request_id,
                min(con_c_time_diff) as min_con_c_time_diff
            from intent_media_sandbox_production.SPYB_MVT_1818_con_c
            where con_c_time_diff between 1 and 2592000
            group by 
                con_request_id) min_con_c_time_diff 
        on con_c.con_request_id = min_con_c_time_diff.con_request_id 
        and con_c.con_c_time_diff = min_con_c_time_diff.min_con_c_time_diff
        group by 
            con_c.c_request_id) att_con
    on c.request_id = att_con.c_request_id;

/* Table 8 - Rollup Table 7 per Ad Call */
drop table if exists intent_media_sandbox_production.SPYB_MVT_1818_c_count;
create table intent_media_sandbox_production.SPYB_MVT_1818_c_count as
    select
        ad_call_request_id,
        advertiser_id,
        count(request_id) as clicks,
        sum(actual_cpc) as revenue,
        sum(clicked_conversions) as clicked_conversions
    from intent_media_sandbox_production.SPYB_MVT_1818_c_con
    group by 
        ad_call_request_id,
        advertiser_id;
    
/* Table 9 - Join Ad Call & Table 8 */
drop table if exists intent_media_sandbox_production.SPYB_MVT_1818_ac_c;
create table intent_media_sandbox_production.SPYB_MVT_1818_ac_c as
    select 
        ac.request_id,
        ac.requested_at,
        ac.site_type,
        ac.product_category_type,
        ac.publisher_user_id,
        ac.mvt_value_1,
        c.advertiser_id,
        c.clicks,
        c.revenue,
        c.clicked_conversions
    from intent_media_sandbox_production.SPYB_MVT_1818_ac ac
    left join intent_media_sandbox_production.SPYB_MVT_1818_c_count c 
    on ac.request_id = c.ad_call_request_id;

drop table if exists intent_media_sandbox_production.SPYB_MVT_1818_user;
create table intent_media_sandbox_production.SPYB_MVT_1818_user as
    select 
        site_type,
        advertiser_id,
        product_category_type,
        mvt_value_1,
        publisher_user_id,
        count(1) as ad_calls,
        count(clicks) as interactions,
        sum(clicks) as clicks,
        sum(revenue) as revenue,
        sum(clicked_conversions) as clicked_conversions
    from intent_media_sandbox_production.SPYB_MVT_1818_ac_c
    group by 
        site_type,
        advertiser_id,
        product_category_type,
        mvt_value_1,
        publisher_user_id;

drop table if exists intent_media_sandbox_production.SPYB_MVT_1818_final;
create table intent_media_sandbox_production.SPYB_MVT_1818_final as
    select 
        site_type,
        advertiser_id,
        product_category_type,
        mvt_value_1,
        count(publisher_user_id) as users,
        sum(ad_calls) as ad_calls,
        sum(interactions) as interactions,
        sum(clicks) as clicks,
        sum(revenue) as revenue,
        sum(clicked_conversions) as clicked_conversions
    from intent_media_sandbox_production.SPYB_MVT_1818_user
    group by 
        site_type,
        advertiser_id,
        product_category_type,
        mvt_value_1;

/* Analysis */
select
mvt_value_1,
sum(users) as users,
sum(ad_calls) as ad_calls,
sum(interactions) as interactions,
sum(clicks) as clicks,
sum(revenue) as revenue,
sum(clicked_conversions) as clicked_conversions
from intent_media_sandbox_production.SPYB_MVT_1818_final
group by 1
order by 1
;

