select 
	bulk_sheet.Advertiser_ID,
	bulk_sheet.Date_Count,
	bulk_sheet.Most_Recent,
	entities.Advertiser_Name
from 
	(select 
		advertiser_id as Advertiser_ID,
		count(date(convert_tz(created_at,'UTC','America/New_York'))) as Date_Count,
		max(date(convert_tz(created_at,'UTC','America/New_York'))) as Most_Recent,
		case when locate(':updated_intent_targets: ',result_summary) > 0 
			then substring_index(substring_index(result_summary,':updated_intent_targets: ',-1),'\n',1) 
			else '' end as updated_intent_targets
	from bulksheet_imports
	where processing_state_type = 'COMPLETED'
	group by advertiser_id) as bulk_sheet
left join
	(select 
		id as Advertiser_ID, 
		name as Advertiser_Name 
	 from entities
	 group by id) as entities
on bulk_sheet.Advertiser_ID = entities.Advertiser_ID
order by entities.Advertiser_Name