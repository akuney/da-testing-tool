$report = @{
    'frequency' = 'Daily';
    'subject' = 'SCA Placement Data Report as of #{$formatted_date}';
    'default_workbook' = 'External SCA Placement Report';
    'assets' = @(
        @{
            'worksheet' = 'CSV Download';
            'filetype' = 'csv';
            'attached' = $true;
            'embedded' = $false;
            'filename' = '#{$yesterday}-SCA_Network_Performance_by_Placement';
            'parameters' = @{
                'Time' = 'Day'
            };
            'column_order' = @('Ad Unit','Month','Quarter','Year','Date','Placement','Product Category Type','Site','Addressable Page Views','Available Impressions','Not Pure, Low Value, Intent Media Traffic Page Views','Clicks','Conversion Value Sum','Conversions','Fillable Pages','Gross Media Revenue','Impressions','Interactions','Low Value Page Views','Net Media Revenue','Not Pure Low Value Page Views','Not Pure Page Views','Page Views','Pages Served','Pure Low Value Page Views','Pure Page Views','Suppressed by Cannibalization Threshold','Suppressed by Click Blackout','Suppressed by No Valid Layout','Suppressed by Route','Suppressed by Unknown Hotel City','Suppressed by Business Rules Other')
        }
    );
    'entities' = @('IM Pub and John')
}