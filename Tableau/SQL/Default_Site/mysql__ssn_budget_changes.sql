select
	date(hsac.created_at) as Date,
	(case 
		when hsac.created_at < first_date.starting_change then 'Wizard'
		when hsac.created_at = first_date.starting_change and e.first_auction_participation is null then 'Wizard'
		when hsac.created_at = first_date.starting_change and e.first_auction_participation is not null then 
			(case 
				when users.`Channel Status` = 'Latter Channel' then 'New Channel'
				else 'Brand New Hotel'
			end)
		else 'Active'
	end) as `Account Status`,
	e.name as `Advertiser Name`,
	e.id as `Advertiser ID`,
	e.ssn_channel_type as `SSN Channel Type`,
	ifnull(imm.name, 'Other') as `Market Name`,
	ifnull(imm.report_segment, 'Other') as `Segment Name`,
	change_type as `Change Type`,
	ifnull(replace(replace(substr(old_settings from (locate('$',old_settings) + 1)), '',', '''),',','') + 0.0, 0.0) as `Old Amount`,
	ifnull(replace(replace(substr(new_settings from (locate('$',new_settings) + 1)), ',', ''),',','') + 0.0, 0.0) as `New Amount`,
	if(change_type = 'Bid Changed', substr(old_settings, 1, locate(':',old_settings) - 1), null) as `Old Bid Type`,
	if(change_type = 'Bid Changed', substr(new_settings, 1, locate(':',new_settings) - 1), null) as `New Bid Type`,
	if(change_type = 'Budget Changed', substr(old_settings, locate(':',old_settings) + 2, locate(',',old_settings) - (locate(':',old_settings) + 2)), null) as `Old Budget Type`,
	if(change_type = 'Budget Changed', substr(new_settings, locate(':',new_settings) + 2, locate(',',new_settings) - (locate(':',new_settings) + 2)), null) as `New Budget Type`,
	users.`Phone Number`,
	users.`User Name`,
	users.`User Email`,
	users.`Channel Status`,
	users.`Is Primary User`,
	users.`Distinct Hotel Properties`,
	users.`Distinct Entities`,
	users.`User First Auction Participation`,
	users.`Strategic Account Type`,
	users.`Known Rotator Type`
from hotel_ssr_advertiser_changes hsac
left join entities e on e.id = hsac.advertiser_id
left join hotel_property_advertisers hpa on hpa.hotel_ssr_advertiser_id = hsac.advertiser_id
left join intent_media_hotel_properties_markets imhpm on imhpm.hotel_property_id = hpa.hotel_property_id
left join intent_media_markets imm on imm.id = imhpm.intent_media_market_id
left join
	(
    select
      advertiser_id,
      max(hsac.created_at) as starting_change
    from hotel_ssr_advertiser_changes hsac
    left join entities e on e.id = hsac.advertiser_id
    where (hsac.created_at <= e.first_auction_participation or e.first_auction_participation is null)
      and hsac.change_type = 'Budget Changed'
    group by advertiser_id
	) first_date
on first_date.advertiser_id = hsac.advertiser_id
left join
	(
    select
      entities_to_users.*,
      (case when entities_to_users.`User ID` = primary_users.primary_user then 1 else 0 end) as `Is Primary User`,
      hotel_count.`Distinct Hotel Properties`,
      hotel_count.`Distinct Entities`,
      hotel_count.`User First Auction Participation`,
      hotel_count.`Strategic Account Type`,
      hotel_count.`Known Rotator Type`
    from
      (
        select
          e.id as `Advertiser ID`,
          e.telephone as `Phone Number`,
          concat(u.first_name, ' ', u.last_name) as `User Name`,
          u.email as `User Email`,
          u.id as `User ID`,
          e.ssn_channel_type as `SSN Channel Type`,
          channel_types.`Channel Status`
        from intent_media_production.entities e
        left join
          (
            select
              e.id as advertiser_id,
              (case
                when multi_channel_hotel_properties.hotel_property_id is null then 'Only Channel'
                when multi_channel_hotel_properties.min_first_auction_participation = e.first_auction_participation then 'First Channel'
                else 'Latter Channel'
              end) as `Channel Status`
            from intent_media_production.entities e
            left join intent_media_production.hotel_property_advertisers hpa on hpa.hotel_ssr_advertiser_id = e.id
            left join
              (
                select
                  hpa.hotel_property_id,
                  min(first_auction_participation) as min_first_auction_participation
                from intent_media_production.hotel_property_advertisers hpa
                left join intent_media_production.entities e on hpa.hotel_ssr_advertiser_id = e.id
                where e.active = 1 and e.entity_type = 'HotelSsrAdvertiser'
                group by hpa.hotel_property_id
                having count(e.id) > 1
              ) multi_channel_hotel_properties
            on multi_channel_hotel_properties.hotel_property_id = hpa.hotel_property_id
            where e.entity_type = 'HotelSsrAdvertiser'
            and e.active = 1
          ) channel_types
        on channel_types.advertiser_id = e.id
        right join intent_media_production.memberships m on m.entity_id = e.id
        right join intent_media_production.users u on u.id = m.user_id
        where entity_type = 'HotelSsrAdvertiser'
        and e.active = 1
        and e.first_auction_participation is not null
        and m.active = 1
      ) entities_to_users
    left join
      (
        select
          m.entity_id as entity_id,
          min(user_id) as primary_user
        from intent_media_production.memberships m
        where m.active = 1
        group by m.entity_id
      ) primary_users
    on entities_to_users.`Advertiser ID` = primary_users.entity_id
    left join
      (
        select
          u.email as `User Email`,
          count(distinct(hpa.hotel_property_id)) as `Distinct Hotel Properties`,
          count(distinct(e.id)) as `Distinct Entities`,
          min(convert_tz(e.first_auction_participation, 'UTC', 'America/New_York')) as `User First Auction Participation`,
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
          end) as `Strategic Account Type`,
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
          end) as `Known Rotator Type`
        from intent_media_production.users u
        left join intent_media_production.memberships m on m.user_id = u.id
        left join intent_media_production.entities e on e.id = m.entity_id
        left join intent_media_production.hotel_property_advertisers hpa on hpa.hotel_ssr_advertiser_id = e.id
        where e.entity_type = 'HotelSsrAdvertiser'
        and e.active = 1
        and m.active = 1
        group by
          concat(u.first_name, ' ', u.last_name),
          u.email
      ) hotel_count
    on entities_to_users.`User Email` = hotel_count.`User Email`
	) users
on users.`Advertiser ID` = hsac.advertiser_id
where change_type = 'Budget Changed'
and date(hsac.created_at) < date(convert_tz(current_timestamp,'UTC','America/New_York'))