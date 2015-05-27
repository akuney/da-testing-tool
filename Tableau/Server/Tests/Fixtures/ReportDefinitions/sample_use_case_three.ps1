$report = @{
    'frequency' = 'Daily';
    'subject' = 'Intent Media #{$entity} AfT Topline Report';
    'default_workbook' = 'External AfT Advertiser Report for Advertisers';
    'assets' = @{
        'advertiser_topline' = @{
            'worksheet' = 'Topline';
            'filetype' = 'png';
            'attached' = $false;
            'embedded' = $true;
            'filename' = 'topline_for_#{$entity}_#{$loop_through_parameter}';
            'parameters' = @{
                'Advertiser' = '#{$entity}';
                'Product Category' = '#{$loop_through_parameter}'
            }
        };
        'advertiser_data' = @{
            'worksheet' = 'CSV Download';
            'filetype' = 'csv';
            'attached' = $true;
            'embedded' = $false;
            'filename' = 'data_for_#{$entity}';
            'parameters' = @{
                'Advertiser' = '#{$entity}';
                'Product Category' = '#{$loop_through_parameter}'
            };
            'column_order' = @("Advertiser","Date","Local Currency","Product Category Type","Clicks","Impressions","Spend","Average Position","Spend in Local Currency","CPC in Local Currency","CPC","CTR")
        }
    };
    'entities' = @{
        'Expedia-Ads' = @('Flights','Hotels');
        'Travelocity-Ads' = @('Flights')
    }
}