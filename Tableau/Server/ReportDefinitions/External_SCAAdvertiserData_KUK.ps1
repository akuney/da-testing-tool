$report = @{
    'frequency' = 'Monday';
    'subject' = 'Intent Media SCA Weekly Snapshot for Kayak Software Corporation - Ads - UK - $formatted_date';
    'default_workbook' = 'External SCA Advertiser Report for Advertisers';
    'assets' = @(
        @{
            'worksheet' = 'Topline';
            'filetype' = 'png';
            'attached' = $false;
            'embedded' = $true;
            'filename' = 'Kayak-Ads-UK-#{$loop_through_parameter}';
            'parameters' = @{
                'Advertiser' = 'Kayak Software Corporation - Ads - UK';
                'Product Category' = '#{$loop_through_parameter}'
            }
        },
        @{
            'worksheet' = 'CSV Download';
            'filetype' = 'csv';
            'attached' = $true;
            'embedded' = $false;
            'filename' = '#{$yesterday}-IntentMedia-Kayak-Ads-UK';
            'parameters' = @{
                'Advertiser' = 'Kayak Software Corporation - Ads - UK';
                'Product Category' = '#{$loop_through_parameter}'
            };
            'column_order' = @("Advertiser","Date","Local Currency","Product Category Type","Clicks","Impressions","Spend","Average Position","Spend in Local Currency","CPC in Local Currency","CPC","CTR")
        }
    );
    'entities' = @{
        'Kayak Software Corporation - Ads - UK' = @('Flights','Hotels')
    }
}
