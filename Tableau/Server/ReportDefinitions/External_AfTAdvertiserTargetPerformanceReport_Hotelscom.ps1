$report = @{
    'frequency' = 'Monday';
    'subject' = 'Intent Media Hotels.com-Ads Campaign Target Performance for #{$formatted_date}';
    'sender' = 'aft-reports@intentmedia.com'; 
    'recipient_cc' = ''
    'recipient_bcc' = ''
    'default_workbook' = 'External SCA Advertiser Report';
    'assets' = @(
        @{
            'worksheet' = 'byTarget';
            'filetype' = 'csv';
            'attached' = $true;
            'embedded' = $false;
            'filename' = '#{$yesterday}-Intent_Media_Hotels.com-Ads_Campaign_Target_Performance';
            'parameters' = @{
                'Advertiser' = '#{$loop_through_parameter}'
            };
            'column_order' = @('Intent Media','Tracking Code 2','Tracking Code','Date','Clicks','CPC','Spend','Ad Group ID','Ad Group','Advertiser Name','Campaign Status','Campaign Tracking Code','Campaign','City','Country','State','URL Override','Click Conversions','Click Revenue','Impressions','Average Position')
        }
    );
    'entities' = @{
        'Hotels.com' = @('Hotels.com-Ads', 'Hotels.com-Ads - UK')
    }
}
