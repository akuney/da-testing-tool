$report = @{
    'frequency' = 'Monday';
    'subject' = 'Intent Media SCA Weekly Snapshot for #{$entity} - Flights - $formatted_date';
    'default_workbook' = 'External SCA Advertiser Report for Advertisers';
    'assets' = @(
        @{
            'worksheet' = 'Topline';
            'filetype' = 'png';
            'attached' = $false;
            'embedded' = $true;
            'filename' = '#{$entity}-Non-Precheck-#{$loop_through_parameter}';
            'parameters' = @{
                'Advertiser' = '#{$entity}';
                'Product Category' = '#{$loop_through_parameter}';
                'Auction Type Parameter' = 'Non-Precheck'
            }
        },
        @{
            'worksheet' = 'Topline';
            'filetype' = 'png';
            'attached' = $false;
            'embedded' = $true;
            'filename' = '#{$entity}-Precheck-#{$loop_through_parameter}';
            'parameters' = @{
                'Advertiser' = '#{$entity}';
                'Product Category' = '#{$loop_through_parameter}';
                'Auction Type Parameter' = 'Precheck'
            }
        },
        @{
            'worksheet' = 'CSV Download';
            'filetype' = 'csv';
            'attached' = $true;
            'embedded' = $false;
            'filename' = '#{$yesterday}-Intent-Media-#{$entity}-Data';
            'parameters' = @{
                'Advertiser' = '#{$entity}';
                'Product Category' = '#{$loop_through_parameter}';
                'Auction Type Parameter' = 'Total'

            };
            'column_order' = @("Advertiser","Date","Local Currency","Product Category Type","Clicks","Impressions","Spend","Average Position","Spend in Local Currency","CPC in Local Currency","CPC","CTR","Auction Type")
        }
    );
    'entities' = @{
        'Expedia-Ads' = @('Flights')
    }
}
