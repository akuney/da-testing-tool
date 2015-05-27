$report = @{
    'frequency' = 'Daily';
    'subject' = 'Intent Media #{$entity} SCA Placement Data Report as of #{$formatted_date}';
    'default_workbook' = 'External SCA Placement Report';
    'assets' = @(
        @{
            'worksheet' = 'DataWithoutSuppressionReasons';
            'filetype' = 'csv';
            'attached' = $true;
            'embedded' = $false;
            'filename' = '#{$yesterday}-Intent-Media-#{$entity}-Placement-Performance';
            'parameters' = @{
                'Publisher' = '#{$entity}'
            };
            'column_order' = @('Ad Unit', 'Month', 'Quarter', 'Year', 'Date', 'Placement', 'Product Category Type', 'Site', 'Addressable Page Views', 'Not Pure, Low Value, Intent Media Traffic Page Views', 'Clicks', 'Gross Media Revenue', 'Interactions', 'Not Pure Low Value Page Views', 'Page Views', 'Pages Served', 'Available eCPM', 'Clicks per Interaction', 'CTR', 'Interaction Rate', 'Served eCPM')
        }
    );
   'entities' = @('Air Fast Tickets', 'Airtickets', 'Airtrade International', 'Hotwire', 'Oversee')
}
