select
	users.*,
	(case when users."User First Auction Participation" = status_changes."First Auction Participation" then 1 else 0 end) as "First Advertiser for User",
	(case when users."User First Auction Participation" = hotel_property_status_changes."First Auction Participation" then 1 else 0 end) as "First Hotel Property for User",
	ifnull(imm.name, 'Other') as "Market Name",
	ifnull(imm.report_segment, 'Other') as "Segment Name",
	hpa.hotel_property_id as "Hotel Property ID",
	z.can_serve_ads as "Can Serve Ads",
	status_changes.Date,
	status_changes."Advertiser Name",
	status_changes."Sold Date",
	status_changes."Is New",
	status_changes."Advertising Status",
	status_changes."Budget Type",
	status_changes."Budget",
	status_changes."Previous Advertising Status",
	status_changes."Previous Budget Type",
	status_changes."Previous Budget",
	(case
		when "Is New" then
			case
				when users."Channel Status" = 'Latter Channel' then 'New Channel'
				else 'Brand New Hotel'
			end
		when "Advertising Status" like '%Active%' and "Previous Advertising Status" like '%Paused%' then
			case
				when "Change Yesterday or Today" then 'Manually Reactivated'
				else 'Reactivated No Traffic'
			end
		when "Advertising Status" = 'Paused with Zeroed Out Non-Recurring Budget' and "Previous Advertising Status" like '%Active%' then 'Paused for Budget'
		when "Advertising Status" = 'Manually Paused' and "Previous Advertising Status" like '%Active%' then 'Manually Paused'
		when "Advertising Status" = 'Paused No Traffic' and "Previous Advertising Status" like '%Active%' then 'Paused No Traffic'
	end) as "Advertising Status Change",
	(case 
		when hotel_property_status_changes."Hotel Property Advertising Status Value" > 0 then 'Active' 
		when hotel_property_status_changes."Hotel Property Advertising Status Value" = 0 then 'Paused'
	end) as "Hotel Property Advertising Status",
	(case 
		when hotel_property_status_changes."Previous Hotel Property Advertising Status Value" > 0 then 'Active' 
		when hotel_property_status_changes."Previous Hotel Property Advertising Status Value" = 0 then 'Paused' 
	end) as "Previous Hotel Property Advertising Status",
	(case 
		when hotel_property_status_changes."Hotel Property Is New Value" > 0 then 'New'
		when hotel_property_status_changes."Hotel Property Advertising Status Change Value" < 0 then 'Paused'
		when hotel_property_status_changes."Hotel Property Advertising Status Change Value" > 0 then 'Reactivated'
	end) as "Hotel Property Advertising Status Change"
from
	(select
		entities_to_users.*,
		(case when entities_to_users."User ID" = primary_users.primary_user then 1 else 0 end) as "Is Primary User",
		hotel_count."Distinct Hotel Properties",
		hotel_count."Distinct Entities",
		hotel_count."User First Auction Participation",
		hotel_count."Strategic Account Type",
		hotel_count."Known Rotator Type"
	from
		(select
			e.id as "Advertiser ID",
			e.telephone as "Phone Number",
			(u.first_name || ' ' || u.last_name) as "User Name",
			u.email as "User Email",
			u.id as "User ID",
			e.ssn_channel_type as "SSN Channel Type",
			channel_types."Channel Status"
		from intent_media_production.entities e
		left join
			(select 
				e.id as advertiser_id,
				(case
					when multi_channel_hotel_properties.hotel_property_id is null then 'Only Channel'
					when multi_channel_hotel_properties.min_first_auction_participation = e.first_auction_participation then 'First Channel' 
					else 'Latter Channel' 
				end) as "Channel Status"
			from intent_media_production.entities e
			left join intent_media_production.hotel_property_advertisers hpa on hpa.hotel_ssr_advertiser_id = e.id
			left join (select
							hpa.hotel_property_id,
							min(first_auction_participation) as min_first_auction_participation
						from intent_media_production.hotel_property_advertisers hpa 
						left join intent_media_production.entities e on hpa.hotel_ssr_advertiser_id = e.id
						where e.active = 1 and e.entity_type = 'HotelSsrAdvertiser'
						group by hpa.hotel_property_id
						having count(e.id) > 1) as multi_channel_hotel_properties
			on multi_channel_hotel_properties.hotel_property_id = hpa.hotel_property_id
			where e.entity_type = 'HotelSsrAdvertiser'
			and e.active = 1) channel_types
		on channel_types.advertiser_id = e.id
		right join intent_media_production.memberships m on m.entity_id = e.id
		right join intent_media_production.users u on u.id = m.user_id 
		where entity_type = 'HotelSsrAdvertiser'
		and e.active = 1
		and e.first_auction_participation is not null
		and m.active = 1) entities_to_users
	left join 
		(select 
			m.entity_id as entity_id, 
			min(user_id) as primary_user 
		from intent_media_production.memberships m
		where m.active = 1
		group by m.entity_id) primary_users 
		on entities_to_users."Advertiser ID" = primary_users.entity_id
	left join
		(select
			u.email "User Email",
			count(distinct(hpa.hotel_property_id)) as "Distinct Hotel Properties",
			count(distinct(e.id)) as "Distinct Entities",
			min(e.first_auction_participation at timezone 'America/New_York') as "User First Auction Participation",
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
		left join intent_media_production.hotel_property_advertisers hpa on hpa.hotel_ssr_advertiser_id = e.id
		where e.entity_type = 'HotelSsrAdvertiser'
		and e.active = 1
		and m.active = 1
		group by
			u.first_name || ' ' || u.last_name,
			u.email) hotel_count
	on entities_to_users."User Email" = hotel_count."User Email") users
	
	
left join

	(select
		sold.date_in_et as Date,
		sold.advertiser_id as "Advertiser ID",
		sold.advertiser_name as "Advertiser Name",
		hpa.hotel_property_id as "Hotel Property ID",
		sold.first_auction_participation_in_et as "First Auction Participation",
		sold.sold_date_in_et as "Sold Date",
		(case when sold.date_in_et = sold.sold_date_in_et then 1 else 0 end) as "Is New",
		(case
			when pas.advertiser_id is not null then 'Active'
			when pas.advertiser_id is null and (budgets.effective_budget < 0.25 and budgets.budget_type = 'MONTHLY') then 'Active with Zeroed Out Recurring Budget'
			when pas.advertiser_id is null and (budgets.effective_budget < 0.25 and budgets.budget_type <> 'MONTHLY') then 'Paused with Zeroed Out Non-Recurring Budget'
			when pas.advertiser_id is null and budgets.effective_budget >= 0.25 then 
				(case 
					when sold.max_change_date >= sold.max_participation_date then 'Manually Paused'
					else 'Paused No Traffic'
				end)
		end) as "Advertising Status",
		budgets.budget_type as "Budget Type",
		budgets.effective_budget as "Budget",
		(sold.max_change_date = sold.date_in_et or sold.previous_max_change_date = (sold.date_in_et - interval '1 day')) as "Change Yesterday or Today",
		(case
			when previous_pas.advertiser_id is not null then 'Active'
			when previous_pas.advertiser_id is null and (previous_budgets.effective_budget < 0.25 and previous_budgets.budget_type = 'MONTHLY') then 'Active with Zeroed Out Recurring Budget'
			when previous_pas.advertiser_id is null and (previous_budgets.effective_budget < 0.25 and previous_budgets.budget_type <> 'MONTHLY') then 'Paused with Zeroed Out Non-Recurring Budget'
			when previous_pas.advertiser_id is null and previous_budgets.effective_budget >= 0.25 then 
				(case 
					when sold.previous_max_change_date >= sold.previous_max_participation_date then 'Manually Paused'
					else 'Paused No Traffic'
				end)
		end) as "Previous Advertising Status",
		previous_budgets.budget_type as "Previous Budget Type",
		previous_budgets.effective_budget as "Previous Budget"
	from
		(select
			dates_sold_hotels_changes.date_in_et,
			dates_sold_hotels_changes.advertiser_id,
			dates_sold_hotels_changes.advertiser_name,
			dates_sold_hotels_changes.first_auction_participation_in_et,
			dates_sold_hotels_changes.sold_date_in_et,
			dates_sold_hotels_changes.max_change_date,
			dates_sold_hotels_changes.previous_max_change_date,
			max(case when pa.date_in_et <= dates_sold_hotels_changes.date_in_et then pa.date_in_et end) as max_participation_date,
			max(case when pa.date_in_et < dates_sold_hotels_changes.date_in_et then pa.date_in_et end) as previous_max_participation_date
		from
			(select
				dates_sold_hotels.date_in_et,
				dates_sold_hotels.advertiser_id,
				dates_sold_hotels.advertiser_name,
				dates_sold_hotels.first_auction_participation_in_et,
				dates_sold_hotels.sold_date_in_et,
				max(case when hsac.date_in_et <= dates_sold_hotels.date_in_et then hsac.date_in_et end) as max_change_date,
				max(case when hsac.date_in_et < dates_sold_hotels.date_in_et then hsac.date_in_et end) as previous_max_change_date
			from
				(select *
				from
					-- dates
					(select 
						distinct(aggregation_level_date_in_et) as date_in_et
					from intent_media_production.participating_advertisers) dates,
					-- sold_hotels
					(select
							id as advertiser_id,
							name as advertiser_name,
							(first_auction_participation at timezone 'America/New_York') as first_auction_participation_in_et,
							date(first_auction_participation at timezone 'America/New_York') as sold_date_in_et
					from intent_media_production.entities
					where entity_type = 'HotelSsrAdvertiser'
						and active = 1 
						and first_auction_participation is not null) sold_hotels
				where sold_hotels.sold_date_in_et <= dates.date_in_et) dates_sold_hotels,
				-- changes	
				(select
					date(created_at at timezone 'UTC' at timezone 'America/New_York') as date_in_et,
					advertiser_id
				from intent_media_production.hotel_ssr_advertiser_changes 
				where change_type = 'Budget Changed'
					or change_type = 'Bid Changed'
					or change_type = 'Campaign Status'
				group by date(created_at at timezone 'UTC' at timezone 'America/New_York'), advertiser_id) hsac
			where hsac.advertiser_id = dates_sold_hotels.advertiser_id
			group by 
				dates_sold_hotels.date_in_et,
				dates_sold_hotels.advertiser_id,
				dates_sold_hotels.advertiser_name,
				dates_sold_hotels.first_auction_participation_in_et,
				dates_sold_hotels.sold_date_in_et) dates_sold_hotels_changes,
			-- participating_advertisers			
			(select
				aggregation_level_date_in_et as date_in_et,
				advertiser_id
			from intent_media_production.participating_advertisers
			group by aggregation_level_date_in_et, advertiser_id) pa
		where pa.advertiser_id = dates_sold_hotels_changes.advertiser_id
		group by
			dates_sold_hotels_changes.date_in_et,
			dates_sold_hotels_changes.advertiser_id,
			dates_sold_hotels_changes.advertiser_name,
			dates_sold_hotels_changes.first_auction_participation_in_et,
			dates_sold_hotels_changes.sold_date_in_et,
			dates_sold_hotels_changes.max_change_date,
			dates_sold_hotels_changes.previous_max_change_date) sold				

	-- get hotel property
	left join intent_media_production.hotel_property_advertisers hpa on hpa.hotel_ssr_advertiser_id = sold.advertiser_id
			
	-- get all participating advertisers for that day
	left join
		(select
			pa.aggregation_level_date_in_et,
			pa.advertiser_id
		from intent_media_production.participating_advertisers pa
		group by 
			pa.aggregation_level_date_in_et,
			pa.advertiser_id
		) pas
	on sold.date_in_et = pas.aggregation_level_date_in_et
		and sold.advertiser_id = pas.advertiser_id
		
		
	-- get all participating advertisers for the previous day
	left join
		(select
			pa.aggregation_level_date_in_et,
			pa.advertiser_id
		from intent_media_production.participating_advertisers pa
		group by 
			pa.aggregation_level_date_in_et,
			pa.advertiser_id
		) previous_pas
	on sold.date_in_et = date(previous_pas.aggregation_level_date_in_et + interval '1 day')
		and sold.advertiser_id = previous_pas.advertiser_id
		
	-- get all budgets for that day
	left join
		(select
			latest.date_in_et,
			latest.advertiser_id,
			effective_budget,
			budget_type
		from
		(select
			date_in_et,
			advertiser_id,
			max(id) as latest_id
		from intent_media_production.historical_budgets
		group by date_in_et, advertiser_id) latest
		left join intent_media_production.historical_budgets hb 
			on latest.date_in_et = hb.date_in_et 
			and latest.advertiser_id = hb.advertiser_id
			and latest.latest_id = hb.id) budgets
	on sold.date_in_et = budgets.date_in_et
		and budgets.advertiser_id = sold.advertiser_id
	
	
	-- get all budgets for the previous day
	left join
		(select
			latest.date_in_et,
			latest.advertiser_id,
			effective_budget,
			budget_type
		from
		(select
			date_in_et,
			advertiser_id,
			max(id) as latest_id
		from intent_media_production.historical_budgets
		group by date_in_et, advertiser_id) latest
		left join intent_media_production.historical_budgets hb 
			on latest.date_in_et = hb.date_in_et 
			and latest.advertiser_id = hb.advertiser_id
			and latest.latest_id = hb.id) previous_budgets
	on sold.date_in_et = date(previous_budgets.date_in_et + interval '1 day')
		and previous_budgets.advertiser_id = sold.advertiser_id) status_changes

on users."Advertiser ID" = status_changes."Advertiser ID"

left join intent_media_production.hotel_property_advertisers hpa on hpa.hotel_ssr_advertiser_id = users."Advertiser ID"
left join intent_media_production.intent_media_hotel_properties_markets imhpm on imhpm.hotel_property_id = hpa.hotel_property_id
left join intent_media_production.intent_media_markets imm on imm.id = imhpm.intent_media_market_id
left join intent_media_production.z_hotel_ssr_advertiser_status z on z.advertiser_id = users."Advertiser ID"

left join

	(select
		Date,
		"Hotel Property ID",
		min("First Auction Participation") as "First Auction Participation",
		min("Is New Value") as "Hotel Property Is New Value",
		max("Advertising Status Value") as "Hotel Property Advertising Status Value",
		max("Previous Advertising Status Value") as "Previous Hotel Property Advertising Status Value",
		sum("Advertising Status Value" - "Previous Advertising Status Value") as "Hotel Property Advertising Status Change Value"
	from
		(select
			sold.date_in_et as Date,
			hpa.hotel_property_id as "Hotel Property ID",
			sold.advertiser_id as "Advertiser ID",
			sold.first_auction_participation_in_et as "First Auction Participation",
			(case when sold.date_in_et = sold.sold_date_in_et then 1 else 0 end) as "Is New Value",
			(case
				when pas.advertiser_id is not null then 1
				when pas.advertiser_id is null and (budgets.effective_budget < 0.25 and budgets.budget_type = 'MONTHLY') then 1
				when pas.advertiser_id is null and (budgets.effective_budget < 0.25 and budgets.budget_type <> 'MONTHLY') then 0
				when pas.advertiser_id is null and budgets.effective_budget >= 0.25 then 0
			end) as "Advertising Status Value",
			(case
				when previous_pas.advertiser_id is not null then 1
				when previous_pas.advertiser_id is null and (previous_budgets.effective_budget < 0.25 and previous_budgets.budget_type = 'MONTHLY') then 1
				when previous_pas.advertiser_id is null and (previous_budgets.effective_budget < 0.25 and previous_budgets.budget_type <> 'MONTHLY') then 0
				when previous_pas.advertiser_id is null and previous_budgets.effective_budget >= 0.25 then 0
			end) as "Previous Advertising Status Value"
		from
			(select *
			from
				-- dates
				(select 
					distinct(aggregation_level_date_in_et) as date_in_et
				from intent_media_production.participating_advertisers) dates,
				-- sold_hotels
				(select
						id as advertiser_id,
						name as advertiser_name,
						(first_auction_participation at timezone 'America/New_York') as first_auction_participation_in_et,
						date(first_auction_participation at timezone 'America/New_York') as sold_date_in_et
				from intent_media_production.entities
				where entity_type = 'HotelSsrAdvertiser'
					and active = 1 
					and first_auction_participation is not null) sold_hotels
			where sold_hotels.sold_date_in_et <= dates.date_in_et) sold				
	
		-- get hotel property
		left join intent_media_production.hotel_property_advertisers hpa on hpa.hotel_ssr_advertiser_id = sold.advertiser_id
		
		-- get all participating advertisers for that day
		left join
			(select
				pa.aggregation_level_date_in_et,
				pa.advertiser_id
			from intent_media_production.participating_advertisers pa
			group by 
				pa.aggregation_level_date_in_et,
				pa.advertiser_id
			) pas
		on sold.date_in_et = pas.aggregation_level_date_in_et
			and sold.advertiser_id = pas.advertiser_id
			
			
		-- get all participating advertisers for the previous day
		left join
			(select
				pa.aggregation_level_date_in_et,
				pa.advertiser_id
			from intent_media_production.participating_advertisers pa
			group by 
				pa.aggregation_level_date_in_et,
				pa.advertiser_id
			) previous_pas
		on sold.date_in_et = date(previous_pas.aggregation_level_date_in_et + interval '1 day')
			and sold.advertiser_id = previous_pas.advertiser_id
			
		-- get all budgets for that day
		left join
			(select
				latest.date_in_et,
				latest.advertiser_id,
				effective_budget,
				budget_type
			from
			(select
				date_in_et,
				advertiser_id,
				max(id) as latest_id
			from intent_media_production.historical_budgets
			group by date_in_et, advertiser_id) latest
			left join intent_media_production.historical_budgets hb 
				on latest.date_in_et = hb.date_in_et 
				and latest.advertiser_id = hb.advertiser_id
				and latest.latest_id = hb.id) budgets
		on sold.date_in_et = budgets.date_in_et
			and budgets.advertiser_id = sold.advertiser_id
		
		
		-- get all budgets for the previous day
		left join
			(select
				latest.date_in_et,
				latest.advertiser_id,
				effective_budget,
				budget_type
			from
			(select
				date_in_et,
				advertiser_id,
				max(id) as latest_id
			from intent_media_production.historical_budgets
			group by date_in_et, advertiser_id) latest
			left join intent_media_production.historical_budgets hb 
				on latest.date_in_et = hb.date_in_et 
				and latest.advertiser_id = hb.advertiser_id
				and latest.latest_id = hb.id) previous_budgets
		on sold.date_in_et = date(previous_budgets.date_in_et + interval '1 day')
			and previous_budgets.advertiser_id = sold.advertiser_id) hotel_status_changes_with_property
	group by Date, "Hotel Property ID") hotel_property_status_changes

on status_changes.Date = hotel_property_status_changes.Date
and status_changes."Hotel Property ID" = hotel_property_status_changes."Hotel Property ID"
and (users."Channel Status" = 'Only Channel' or users."Channel Status" = 'First Channel')

where status_changes.Date < date(current_timestamp at timezone 'UTC' at timezone 'America/New_York')