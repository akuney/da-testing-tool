$report = @{
    'frequency' = 'Daily';
    'subject' = 'Intent Media OWW Campaign Performance for #{$formatted_date}';
    'sender' = 'aft-reports@intentmedia.com'; 
    'recipient_cc' = ''
    'recipient_bcc' = ''
    'default_workbook' = 'External SCA OWW Advertiser Report';
    'assets' = @(
        @{
            'worksheet' = 'CSVNoConversions';
            'filetype' = 'csv';
            'attached' = $true;
            'embedded' = $false;
            'filename' = '#{$yesterday}-Intent_Media_Orbitz_Campaign_Performance';
            'parameters' = @{
                'Time Frame' = 'Week'
            };
            'column_order' = @('Advertiser Name','Campaign Status','Campaign','Date','Product Category','Tracking Code','Clicks','Impressions','Spend','Average Position','Impression Share','CPC','CTR','eCPM')
        }
    );
    'entities' = @('Intent Media')
}
