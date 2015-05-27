$report = @{
    'frequency' = 'Daily';
    'subject' = 'Intent Media #{$entity} Cross Product Report as of #{$formatted_date}';
    'default_workbook' = 'Cross Product Dashboard';
    'assets' = @(
        @{
            'worksheet' = 'CSV';
            'filetype' = 'csv';
            'attached' = $true;
            'embedded' = $false;
            'filename' = '#{$yesterday}-Intent-Media-#{$entity}-Cross-Product-Performance';
            'parameters' = @{
                'Site' = '#{$entity}'
            };
            'column_order' = @('Date', 'Year', 'Quarter', 'Month', 'Week', 'Network', 'Product', 'Publisher', 'Site', 'Type of Ad Unit', 'Ad Unit', 'Page Views', 'Percent Addressable', 'Addressable Pages', 'Fill Rate', 'Pages Served', 'Interactions', 'CPI', 'CTR', 'Clicks', 'CPC', 'Gross Media Revenue', 'Available eCPM')
        }
    );
   'entities' = @('ebookers')
}