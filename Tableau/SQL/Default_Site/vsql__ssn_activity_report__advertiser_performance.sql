select
	users."User Name",
	users."User Email",
	users."Is Primary User",
	users."Distinct Hotels",
	users."Strategic Account Type",
	users."Known Rotator Type",
	users."Phone Number",
	users."First Auction Participation",
	performance.*
from
	(select
		entities_to_users.*,
		(case when entities_to_users."User ID" = primary_users.primary_user then 1 else 0 end) as "Is Primary User",
		hotel_count."Distinct Hotels",
		hotel_count."Strategic Account Type",
		hotel_count."Known Rotator Type"
	from
		(select
			e.id as "Advertiser ID",
			e.telephone as "Phone Number",
			(u.first_name || ' ' || u.last_name) as "User Name",
			u.email as "User Email",
			u.id as "User ID",
			e.first_auction_participation as "First Auction Participation"
		from intent_media_production.entities e
		right join intent_media_production.memberships m on m.entity_id = e.id
		right join intent_media_production.users u on u.id = m.user_id 
		where entity_type = 'HotelSsrAdvertiser'
		and e.active = 1
		and e.first_auction_participation is not null
		and m.active = 1) entities_to_users
	left join 
		(select 
			m2.entity_id as entity_id, 
			min(user_id) as primary_user 
		from intent_media_production.memberships m2
		where m2.active = 1
		group by m2.entity_id) primary_users 
		on entities_to_users."Advertiser ID" = primary_users.entity_id
	left join
		(select
			u.email "User Email",
			count(e.name) as "Distinct Hotels",
			(case when 
				sum(case when 
						 u.email = 'onlinemarketing@aquaresorts.com' or
						 u.email = 'pahler@aquaresorts.com' or
						 u.email = 'diego@q9ads.com' or
						 u.email = 'michael.anthony@orbitz.com' or
						 u.email = 'Michael.Anthony@starwoodhotels.com' or
						 u.email = 'matta@hzdg.com' or
						 u.email = 'lbarnes@holidayinnclub.com' or
						 u.email = 'lbayles@holidayinnclub.com' or
						 u.email = 'rentalmarketing@holidayinnclub.com' or
						 u.email = 'editor@courtyardsd.com' or
						 u.email = 'editor@hardrockhotelsd.com' or
						 u.email = 'editor@jollyrogerhotel.com' or
						 u.email = 'editor@portofinoinnanaheim.com' or
						 u.email = 'editor@ramadaplazasd.com' or
						 u.email = 'sagarb@tarsadia.com' or
						 u.email = 'robertb@hcdg.com' or
						 u.email = 'robertb@hzdg.com' or
						 u.email = 'santiago.casillas@posadas.com' or
						 u.email = 'brad.chamberlin@rrpartners.com' or
						 u.email = 'danushka.chandrasekaram@hilton.com' or
						 u.email = 'mchapur@allinclusivecollection.com' or
						 u.email = 'keoni@columbiasussex.com' or
						 u.email = 'elli@q9ads.com' or
						 u.email = 'sclough@holidayinnclub.com' or
						 u.email = 'amyd@hzdg.com' or
						 u.email = 'tfarber@hvmg.com' or
						 u.email = 'todd@hiresortlbv.com' or
						 u.email = 'lakota.forosisky@hilton.com' or
						 u.email = 'lakota.forosisky@hyatt.com' or
						 u.email = 'cfoster@mckibbonhotels.com' or
						 u.email = 'krista.hallecy@standingdog.com' or
						 u.email = 'matthew.harrison@hyatt.com' or
						 u.email = 'carol.helbling@disney.com' or
						 u.email = 'clive.heron@wyn.com' or
						 u.email = 'clive.heron@wyndhamvo.com' or
						 u.email = 'jhope@jhmhotels.com' or
						 u.email = 'joanna@q9ads.com' or
						 u.email = 'george.hunter@posadas.com' or
						 u.email = 'lhynie@troplv.com' or
						 u.email = 'eizquierdo@hrhaic.com' or
						 u.email = 'eizquierdo@meridiencancun.com.mx' or
						 u.email = 'seank@investorshm.com' or
						 u.email = 'seankane@q.com' or
						 u.email = 'price.karr@wynnlasvegas.com' or
						 u.email = 'robyn.kinard@wynnlasvegas.com' or
						 u.email = 'fkreitman@colwenhotels.com' or
						 u.email = 'tlabie@hrhvegas.com' or
						 u.email = 'CLaRosa@jhmhotels.com' or
						 u.email = 'brian@mckibbonhotels.com' or
						 u.email = 'rachel.lerner@starwoodhotels.com' or
						 u.email = 'gordon@revenueperformance.com' or
						 u.email = 'Orbitz@revenueperformance.com' or
						 u.email = 'Travelocity@revenueperformance.com' or
						 u.email = 'angela.lundquist@cosmopolitanlasvegas.com' or
						 u.email = 'meredithm@hcdg.com' or
						 u.email = 'meredithm@hzdg.com' or
						 u.email = 'benensonrofar.orbitz@buildrevpar.com' or
						 u.email = 'chatham.orbitz@buildrevpar.com' or
						 u.email = 'smaines@ih-corp.com' or
						 u.email = 'dmalbrough@caesars.com' or
						 u.email = 'dmalbrough@lasvegas.harrahs.com' or
						 u.email = 'jmandarino@thelvh.com' or
						 u.email = 'susanm@myhospitality.org' or
						 u.email = 'susan_mandarino@myhhotels.com' or
						 u.email = 'susan_mandarino@myhhotels.com' or
						 u.email = 'jmandarino@thelvh.com' or
						 u.email = 'nmason@hrhvegas.com' or
						 u.email = 'nmason@hrhvegas.com' or
						 u.email = 'ross.moore@hilton.com' or
						 u.email = 'lisa.moore@marriott.com' or
						 u.email = 'eric.ettlin@marriott.com' or
						 u.email = 'wmitchell@imagehotels.net' or
						 u.email = 'ross.moore@hilton.com' or
						 u.email = 'liana@antlersvail.com' or
						 u.email = 'jusmoore@hotwire.com' or
						 u.email = 'alejandro.moxey@marriott.com' or
						 u.email = 'alejandro.moxey@marriott.com' or
						 u.email = 'Erin.Naeve@cosmopolitanlasvegas.com' or
						 u.email = 'Erin.Naeve@cosmopolitanlasvegas.com' or
						 u.email = 'knichols@jhmhotels.com' or
						 u.email = 'knichols@jhmhotels.com' or
						 u.email = 'rory.peska@sheraton.com' or
						 u.email = 'rory.peska@standingdog.com' or
						 u.email = 'rorypeska@remingtonhotels.com' or
						 u.email = 'umercado@jeweldunnsriverresort.com' or
						 u.email = 'rory.peska@sheraton.com' or
						 u.email = 'umercado@jeweldunnsriverresort.com' or
						 u.email = 'rory.peska@standingdog.com' or
						 u.email = 'rorypeska@remingtonhotels.com' or
						 u.email = 'rob.phillips@standingdog.com' or
						 u.email = 'dphillips@montereyplazahotel.com' or
						 u.email = 'rob.phillips@standingdog.com' or
						 u.email = 'info@beaconlightguesthouse.com' or
						 u.email = 'sphillips@himonroeville.com' or
						 u.email = 'darren@mhgrp.net' or
						 u.email = 'lpisan@hrhvegas.com' or
						 u.email = 'lpisan@hrhvegas.com' or
						 u.email = 'Lalcuri@holidayinnclub.com' or
						 u.email = 'mpowell@holidayinnclub.com' or
						 u.email = 'Lalcuri@holidayinnclub.com' or
						 u.email = 'willpowell@myrtlewoodvillas.com' or
						 u.email = 'Paul.Powell@whotels.com' or
						 u.email = 'mpowell@holidayinnclub.com' or
						 u.email = 'lee@cpfcc.com' or
						 u.email = 'jarodrig@holidayinnclub.com' or
						 u.email = 'reservations@rentavilladisney.com' or
						 u.email = 'lirodriguez@bernhotelspanama.com' or
						 u.email = 'jarodrig@holidayinnclub.com' or
						 u.email = 'giselda.schreurs@posadas.com' or
						 u.email = 'giselda.schreurs@posadas.com' or
						 u.email = 'jscott@colwenhotels.com' or
						 u.email = 'jscott@colwenhotels.com' or
						 u.email = 'emily@kayak.com' or
						 u.email = 'baymontinngm@gmail.com' or
						 u.email = 'baymontinngm.dtw@gmail.com' or
						 u.email = 'leigh.silkunas@kimptonhotels.com' or
						 u.email = 'leigh.silkunas@kimptonhotels.com' or
						 u.email = 'Kristina.smith@standingdog.com' or
						 u.email = 'gm@castlehillresort.com' or
						 u.email = 'jertl@wigwamresort.com' or
						 u.email = 'sarah.smith@acepllc.com' or
						 u.email = 'nicole.smith2@ihg.com' or
						 u.email = 'brandons@arizonabiltmore.com' or
						 u.email = 'ssmith2@destinationhotels.com' or
						 u.email = 'hiexcapecoral@gmail.com' or
						 u.email = 'jsmith@fhginc.com' or
						 u.email = 'jil@hollywoodbeachgolf.com' or
						 u.email = 'jason.smith2@hilton.com' or
						 u.email = 'pam.smith@hyatt.com' or
						 u.email = 'NSmith@montelucia.com' or
						 u.email = 'francess@naplesgolfresort.com' or
						 u.email = 'Kristina.smith@standingdog.com' or
						 u.email = 'Daniel.smith@regenthotels.com' or
						 u.email = 'michelle.smith2@hilton.com' or
						 u.email = 'tsmith@zimmerman.com' or
						 u.email = 'starla.smith@hyattfrenchquarter.com' or
						 u.email = 'ksmith@highwiremarketing.com' or
						 u.email = 'ahdream.smith@digitas.com' or
						 u.email = 'ashsmith@hotels.com' or
						 u.email = 'k_smith9@hotmail.com' or
						 u.email = 'generalmanager@baymontew.com' or
						 u.email = 'Lee.Smith@marriott.com' or
						 u.email = 'kspitz@hrhvegas.com' or
						 u.email = 'kristin.spitz@hyatt.com' or
						 u.email = 'kspitz@hrhvegas.com' or
						 u.email = 'dtimmons-pixler@thelvh.com' or
						 u.email = 'dtimmons-pixler@thelvh.com' or
						 u.email = 'lori.warwick@pyramidhotelgroup.com' or
						 u.email = 'lori.warwick@pyramidhotelgroup.com' or
						 u.email = 'nic@q9ads.com' or
						 u.email = 'nic@q9ads.com' or
						 u.email = 'roger@planbhospitality.com' or
						 u.email = 'stephanie.young@standingdog.com' or
						 u.email = 'roger@planbhospitality.com' or
						 u.email = 'charlottedyoung@gmail.com' or
						 u.email = 'stephanie.young@standingdog.com' or
						 u.email = 'nyoung@loewshotels.com' or
						 u.email = 'david.zschernig@disney.com' or
						 u.email = 'david.zschernig@disney.com'
					then 1
					else 0
				end) > 0 then 'Strategic Accounts'
				else 'Other Accounts'
			end) as "Strategic Account Type",
			(case when 
				sum(case when 
						 u.email = 'ann@bgsolutions.me' or
						 u.email = 'brian@mckibbonhotels.com' or
						 u.email = 'cfoster@mckibbonhotels.com' or
						 u.email = 'charles.bido@melia.com' or
						 u.email = 'elli@q9ads.com' or
						 u.email = 'jalleruzzo@rpmadv.com' or
						 u.email = 'joanna@q9ads.com' or
						 u.email = 'laine.mizuno@astonhotels.com' or
						 u.email = 'lmarino@rpmadv.com' or
						 u.email = 'madisonwest.wi@americinn.com' or
						 u.email = 'smetovic@rpmadv.com'
					then 1 else 0
				end) > 0 then 'Known Rotators'
				else 'Other Accounts'
			end) as "Known Rotator Type"
		from intent_media_production.users u
		left join intent_media_production.memberships m on m.user_id = u.id
		left join intent_media_production.entities e on e.id = m.entity_id
		where e.entity_type = 'HotelSsrAdvertiser'
		and e.active = 1
		and e.first_auction_participation is not null
		and m.active = 1
		group by
			u.first_name || ' ' || u.last_name,
			u.email) hotel_count
	on entities_to_users."User Email" = hotel_count."User Email") users
left join 
	(select 
		isra.aggregation_level_date_in_et as Date,
		e.ssn_channel_type as "SSN Channel Type",
		e.last_auction_participation as "Last Auction Participation",
		e.name as "Advertiser Name",
		isra.advertiser_id as "Advertiser ID",
		hpa.hotel_property_id as "Hotel Property ID",
		imhpm.intent_media_market_id as "Market ID", 	
		ifnull(imm.name , 'Other') as "Market Name",
		ifnull(imm.report_segment, 'Other') as "Segment Name",
		z.can_serve_ads as "Can Serve Ads",
		(case isra.advance_purchase_range_type
			when 'WEEKDAY_TRAVEL_LESS_THAN_OR_EQUAL_TO_21_DAYS' then 'Weekdays within 21 Days'
			when 'WEEKEND_TRAVEL_LESS_THAN_OR_EQUAL_TO_21_DAYS' then 'Weekends within 21 Days'
			when 'WEEKDAY_TRAVEL_GREATER_THAN_21_DAYS' then 'Weekdays 22+ Days Away'
			when 'WEEKEND_TRAVEL_GREATER_THAN_21_DAYS' then 'Weekends 22+ Days Away'
			when 'DATELESS' then 'Dateless'
			else isra.advance_purchase_range_type
		end) as "Travel Window",
		atwra.click_count as "Clicks",
		atwra.click_conversion_count as "Click Conversions",
		atwra.actual_cpc_sum as Spend,
		atwra.click_conversion_value_sum as "Click Conversion Value Sum",
		atwra.exposed_conversion_count as "Exposed Conversion Count",
		atwra.exposed_conversion_value_sum as "Exposed Conversion Value Sum",
		atwra.click_room_nights_sum as "Click Room Nights Sum",
		atwra.exposed_room_nights_sum as "Exposed Room Nights Sum",
		sum(isra.impression_count) as "Impressions", 
		sum(isra.filtered_ad_count) as "Filtered Ads", 
		sum(isra.filtered_ad_for_budget_count) as "Filtered Ads (Budget)", 
		sum(isra.filtered_ad_for_bid_count) as "Filtered Ads (Bid)",
		sum(isra.filtered_ad_for_hotel_unavailable_count) as "Filtered Ads (Hotel Unavailable)"
	from intent_media_production.impression_share_report_aggregations isra
	left join intent_media_production.entities e on e.id = isra.advertiser_id
	left join intent_media_production.hotel_property_advertisers hpa on hpa.hotel_ssr_advertiser_id = e.id
	left join intent_media_production.intent_media_hotel_properties_markets imhpm on imhpm.hotel_property_id = hpa.hotel_property_id
	left join intent_media_production.intent_media_markets imm on imm.id = imhpm.intent_media_market_id
	left join intent_media_production.z_hotel_ssr_advertiser_status z on isra.advertiser_id = z.advertiser_id
	left join 
		(select
			date(aggregation_level at timezone 'UTC' at timezone 'America/New_York') as date_in_et,
			advertiser_id,
			advance_purchase_range_type,
			sum(impression_count) as impression_count,
			sum(click_count) as click_count,
			sum(click_conversion_count) as click_conversion_count,
			sum(actual_cpc_sum) as actual_cpc_sum,
			sum(click_conversion_value_sum) as click_conversion_value_sum,
			sum(exposed_conversion_count) as exposed_conversion_count,
			sum(exposed_conversion_value_sum) as exposed_conversion_value_sum,
			sum(click_room_nights_sum) as click_room_nights_sum,
			sum(exposed_room_nights_sum) as exposed_room_nights_sum
		from intent_media_production.advertiser_travel_window_report_aggregations
		where date(aggregation_level at timezone 'UTC' at timezone 'America/New_York') <  date(current_timestamp at timezone 'UTC' at timezone 'America/New_York')
		group by date(aggregation_level at timezone 'UTC' at timezone 'America/New_York'), advertiser_id, advance_purchase_range_type) atwra
		on atwra.advertiser_id = isra.advertiser_id 
			and atwra.advance_purchase_range_type = isra.advance_purchase_range_type
			and  atwra.date_in_et = isra.aggregation_level_date_in_et
	where isra.aggregation_level_date_in_et < date(current_timestamp at timezone 'UTC' at timezone 'America/New_York')
		and e.active = 1
	group by isra.aggregation_level_date_in_et, 
		e.name, 
		e.ssn_channel_type,
		e.last_auction_participation,
		isra.advertiser_id,
		hpa.hotel_property_id,
		imhpm.intent_media_market_id,
		ifnull(imm.report_segment, 'Other'),
		ifnull(imm.name , 'Other'),
		z.can_serve_ads,
		isra.advance_purchase_range_type,
		atwra.impression_count,
		atwra.click_count,
		atwra.click_conversion_count,
		atwra.actual_cpc_sum,
		atwra.click_conversion_value_sum,
		atwra.exposed_conversion_count,
		atwra.exposed_conversion_value_sum,
		atwra.click_room_nights_sum,
		atwra.exposed_room_nights_sum) performance
on users."Advertiser ID" = performance."Advertiser ID"