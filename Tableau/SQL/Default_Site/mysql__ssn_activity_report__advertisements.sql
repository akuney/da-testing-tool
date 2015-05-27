select
	a.id as "Advertisement ID",
	c.advertiser_id as "Advertiser ID"
from advertisements a
left join ad_groups ag on ag.id = a.ad_group_id
left join campaigns c on c.id = ag.campaign_id
left join entities e on e.id = c.advertiser_id
where e.ssn_channel_type = 'OTA'
and a.deleted = 0
and a.paused = 0