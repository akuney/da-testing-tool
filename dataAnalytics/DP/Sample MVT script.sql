drop table if exists intent_media_sandbox_production.YB_MVT_sample_ac;
create table intent_media_sandbox_production.YB_MVT_sample_ac as
	select 
		request_id,
		requested_at,
		publisher_user_id,
		site_type,
		ad_unit_id,
		multivariate_version_id,
		(case instr(multivariate_test_attributes_variable,'TOASTER',1) 
			when 0 then 'Not_found' 
			else 
				substr(multivariate_test_attributes_variable,instr(multivariate_test_attributes_variable,'FLIGHTS_INTER_CARD_LINK_DISPLAY',1) + length('FLIGHTS_INTER_CARD_LINK_DISPLAY') + 3,
				case instr(multivariate_test_attributes_variable,'","',INSTR(multivariate_test_attributes_variable,'FLIGHTS_INTER_CARD_LINK_DISPLAY',1)) 
					when 0 then length(multivariate_test_attributes_variable) - instr(multivariate_test_attributes_variable,'FLIGHTS_INTER_CARD_LINK_DISPLAY',1) - length('FLIGHTS_INTER_CARD_LINK_DISPLAY') - 3 
					else instr(multivariate_test_attributes_variable,'","',instr(multivariate_test_attributes_variable,'FLIGHTS_INTER_CARD_LINK_DISPLAY',1)) - instr(multivariate_test_attributes_variable,'FLIGHTS_INTER_CARD_LINK_DISPLAY',1) - length('FLIGHTS_INTER_CARD_LINK_DISPLAY') - 3 
				end) 
		end) as mvt_value_1,
		(case instr(multivariate_test_attributes_variable,'FLIGHTS_RIGHT_RAIL_LINK_DISPLAY',1) 
			when 0 then 'Not_found' 
			else 
				substr(multivariate_test_attributes_variable,instr(multivariate_test_attributes_variable,'FLIGHTS_RIGHT_RAIL_LINK_DISPLAY',1) + length('FLIGHTS_RIGHT_RAIL_LINK_DISPLAY') + 3,
				case instr(multivariate_test_attributes_variable,'","',instr(multivariate_test_attributes_variable,'FLIGHTS_RIGHT_RAIL_LINK_DISPLAY',1)) 
					when 0 then length(multivariate_test_attributes_variable) - instr(multivariate_test_attributes_variable,'FLIGHTS_RIGHT_RAIL_LINK_DISPLAY',1) - length('FLIGHTS_RIGHT_RAIL_LINK_DISPLAY') - 4 
					else instr(multivariate_test_attributes_variable,'","',instr(multivariate_test_attributes_variable,'FLIGHTS_RIGHT_RAIL_LINK_DISPLAY',1)) - instr(multivariate_test_attributes_variable,'FLIGHTS_RIGHT_RAIL_LINK_DISPLAY',1) - length('FLIGHTS_RIGHT_RAIL_LINK_DISPLAY') - 3 
				end) 
		end) as mvt_value_2
	from intent_media_log_data_production.ad_calls
	where requested_at_date_in_et between '2014-04-16' and '2014-05-30' /* Get From select distinct requested_at_date_in_et from intent_media_log_data_production.ad_calls where multivariate_version_id = XXX */
		and ip_address_blacklisted = 0
		and site_id = 2 /* get from select id from intent_media_production.sites where display_name like '%something%' */
		and ad_unit_type = 'CT'
	    and outcome_type = 'SERVED'
	    and multivariate_version_id = XXX;

drop table if exists intent_media_sandbox_production.YB_MVT_sample_c;
create table intent_media_sandbox_production.YB_MVT_sample_c as
	select 
		ad_call_request_id,
		count(request_id) as clicks,
		sum(actual_cpc) as revenue,
		sum(case placement_type WHEN 'INTER_CARD' THEN 1 ELSE 0 END) as IC_clicks,
		sum(case placement_type WHEN 'INTER_CARD' THEN actual_cpc ELSE 0 END) as IC_revenue,
		sum(case placement_type WHEN 'RIGHT_RAIL' THEN 1 ELSE 0 END) as RR_clicks,
		sum(case placement_type WHEN 'RIGHT_RAIL' THEN actual_cpc ELSE 0 END) as RR_revenue,
		sum(case placement_type WHEN 'EXIT_UNIT' THEN 1 ELSE 0 END) as EU_clicks,
		sum(case placement_type WHEN 'EXIT_UNIT' THEN actual_cpc ELSE 0 END) as EU_revenue,
		sum(case placement_type WHEN 'FOOTER' THEN 1 ELSE 0 END) as F_clicks,
		sum(case placement_type WHEN 'FOOTER' THEN actual_cpc ELSE 0 END) as F_revenue,
		sum(case placement_type WHEN 'IN_CARD' THEN 1 ELSE 0 END) as I_clicks,
		sum(case placement_type WHEN 'IN_CARD' THEN actual_cpc ELSE 0 END) as I_revenue,
		sum(case placement_type WHEN 'SEARCH_FORM' THEN 1 ELSE 0 END) as SF_clicks,
		sum(case placement_type WHEN 'SEARCH_FORM' THEN actual_cpc ELSE 0 END) as SF_revenue,
		sum(case placement_type WHEN 'MINI_CARD' THEN 1 ELSE 0 END) as MC_clicks,
		sum(case placement_type WHEN 'MINI_CARD' THEN actual_cpc ELSE 0 END) as MC_revenue,
		sum(case placement_type WHEN 'FORM_COMPARE' THEN 1 ELSE 0 END) as FC_clicks,
		sum(case placement_type WHEN 'FORM_COMPARE' THEN actual_cpc ELSE 0 END) as FC_revenue,
		sum(case placement_type WHEN 'TOP_CARD' THEN 1 ELSE 0 END) as TC_clicks,
		sum(case placement_type WHEN 'TOP_CARD' THEN actual_cpc ELSE 0 END) as TC_revenue,
		sum(case placement_type WHEN 'DIALOG' THEN 1 ELSE 0 END) as D_clicks,
		sum(case placement_type WHEN 'DIALOG' THEN actual_cpc ELSE 0 END) as D_revenue,
		sum(case placement_type WHEN 'TOASTER' THEN 1 ELSE 0 END) as T_clicks,
		sum(case placement_type WHEN 'TOASTER' THEN actual_cpc ELSE 0 END) as T_revenue
	from intent_media_log_data_production.clicks
	where requested_at_date_in_et between '2014-04-16' and date('2014-05-30' + interval '24 hours') /* Get From select distinct requested_at_date_in_et from intent_media_log_data_production.ad_calls where multivariate_version_id = XXX */
		and ip_address_blacklisted = 0
		and fraudulent = 0
		and site_type = 'ORBITZ_GLOBAL' /* get from select name from intent_media_production.sites where display_name like '%something%' */
		and multivariate_version_id = XXX
	group by 
		ad_call_request_id;

drop table if exists intent_media_sandbox_production.YB_MVT_sample_final;
create table intent_media_sandbox_production.YB_MVT_sample_final as
	select 
		mvt_value_1,
		mvt_value_2,
		count(publisher_user_id) as users,
		sum(ad_calls) as ad_calls,
		sum(ad_calls * ad_calls) as ad_calls_2,
		sum(interactions) as interactions,
		sum(interactions * interactions) as interactions_2_u,
		sum(clicks) as clicks,
		sum(clicks_2_ac) as clicks_2_ac,
		sum(clicks * clicks) as clicks_2_u,
		sum(revenue) as revenue,
		sum(revenue_2_ac) as revenue_2_ac,
		sum(revenue * revenue) as revenue_2_u,
		sum(IC_clicks) as IC_clicks,
		sum(IC_revenue) as IC_revenue,
		sum(RR_clicks) as RR_clicks,
		sum(RR_revenue) as RR_revenue,
		sum(EU_clicks) as EU_clicks,
		sum(EU_revenue) as EU_revenue,
		sum(F_clicks) as F_clicks,
		sum(F_revenue) as F_revenue,
		sum(I_clicks) as I_clicks,
		sum(I_revenue) as I_revenue,
		sum(SF_clicks) as SF_clicks,
		sum(SF_revenue) as SF_revenue,
		sum(MC_clicks) as MC_clicks,
		sum(MC_revenue) as MC_revenue,
		sum(FC_clicks) as FC_clicks,
		sum(FC_revenue) as FC_revenue,
		sum(TC_clicks) as TC_clicks,
		sum(TC_revenue) as TC_revenue,
		sum(D_clicks) as D_clicks,
		sum(D_revenue) as D_revenue,
		sum(T_clicks) as T_clicks,
		sum(T_revenue) as T_revenue
	from (
		select 
			mvt_value_1,
			mvt_value_2,
			publisher_user_id,
			count(request_id) as ad_calls,
			count(clicks) as interactions,
			sum(clicks) as clicks,
			sum(clicks * clicks) as clicks_2_ac,
			sum(revenue) as revenue,
			sum(revenue * revenue) as revenue_2_ac,
			sum(IC_clicks) as IC_clicks,
			sum(IC_revenue) as IC_revenue,
			sum(RR_clicks) as RR_clicks,
			sum(RR_revenue) as RR_revenue,
			sum(EU_clicks) as EU_clicks,
			sum(EU_revenue) as EU_revenue,
			sum(F_clicks) as F_clicks,
			sum(F_revenue) as F_revenue,
			sum(I_clicks) as I_clicks,
			sum(I_revenue) as I_revenue,
			sum(SF_clicks) as SF_clicks,
			sum(SF_revenue) as SF_revenue,
			sum(MC_clicks) as MC_clicks,
			sum(MC_revenue) as MC_revenue,
			sum(FC_clicks) as FC_clicks,
			sum(FC_revenue) as FC_revenue,
			sum(TC_clicks) as TC_clicks,
			sum(TC_revenue) as TC_revenue,
			sum(D_clicks) as D_clicks,
			sum(D_revenue) as D_revenue,
			sum(T_clicks) as T_clicks,
			sum(T_revenue) as T_revenue
		from (
			select 
				ac.request_id,
				ac.requested_at,
				ac.publisher_user_id,
				ac.site_type,
				ac.ad_unit_id,
				ac.multivariate_version_id,
				ac.mvt_value_1,
				ac.mvt_value_2,
				c.clicks,
				c.revenue,
				c.IC_clicks,
				c.IC_revenue,
				c.RR_clicks,
				c.RR_revenue,
				c.EU_clicks,
				c.EU_revenue,
				c.F_clicks,
				c.F_revenue,
				c.I_clicks,
				c.I_revenue,
				c.SF_clicks,
				c.SF_revenue,
				c.MC_clicks,
				c.MC_revenue,
				c.FC_clicks,
				c.FC_revenue,
				c.TC_clicks,
				c.TC_revenue,
				c.D_clicks,
				c.D_revenue,
				c.T_clicks,
				c.T_revenue
			from intent_media_sandbox_production.YB_MVT_sample_ac ac
			left join intent_media_sandbox_production.YB_MVT_sample_c c 
				on ac.request_id = c.ad_call_request_id
			) ac_c
		group by 
			mvt_value_1,
			mvt_value_2,
			publisher_user_id
		) user_agg
	group by  
		mvt_value_1,
		mvt_value_2;

/* Analysis */

select * 
from intent_media_sandbox_production.YB_MVT_sample_final
order by 
	mvt_value_1 desc, 
	mvt_value_2 desc;
