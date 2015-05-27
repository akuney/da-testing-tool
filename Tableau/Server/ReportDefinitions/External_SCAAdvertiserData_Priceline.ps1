$report = @{
    'frequency' = 'Monday';
    'subject' = 'Intent Media SCA Weekly Snapshot for Priceline - $formatted_date';
    'default_workbook' = 'External SCA Advertiser Report for Advertisers';
    'assets' = @(
        @{
            'worksheet' = 'Topline';
            'filetype' = 'png';
            'attached' = $false;
            'embedded' = $true;
            'filename' = 'Priceline-Ads-Non-Precheck-Flights-Topline';
            'parameters' = @{
                'Advertiser' = 'Priceline-Ads';
                'Product Category' = 'Flights';
                'Auction Type Parameter' = 'Non-Precheck'
            }
        },
        @{
            'worksheet' = 'Topline';
            'filetype' = 'png';
            'attached' = $false;
            'embedded' = $true;
            'filename' = 'Priceline-Ads-Precheck-Flights-Topline';
            'parameters' = @{
                'Advertiser' = 'Priceline-Ads';
                'Product Category' = 'Flights';
                'Auction Type Parameter' = 'Precheck'
            }
        },
        @{
            'worksheet' = 'Topline';
            'filetype' = 'png';
            'attached' = $false;
            'embedded' = $true;
            'filename' = 'Priceline-Ads-Hotels-Topline';
            'parameters' = @{
                'Advertiser' = 'Priceline-Ads';
                'Product Category' = 'Hotels'
            }
        },
        @{
            'worksheet' = 'Topline';
            'filetype' = 'png';
            'attached' = $false;
            'embedded' = $true;
            'filename' = 'Priceline-UK-Hotels-Topline';
            'parameters' = @{
                'Advertiser' = 'Priceline+-+UK';
                'Product Category' = 'Hotels'
            }
        },
        @{
            'worksheet' = 'Topline';
            'filetype' = 'png';
            'attached' = $false;
            'embedded' = $true;
            'filename' = 'Priceline-Meta-Topline';
            'parameters' = @{
                'Advertiser' = 'Priceline+-+US+-+Meta';
                'Product Category' = 'PPA'
            }
        },
        @{
            'worksheet' = 'Topline';
            'filetype' = 'png';
            'attached' = $false;
            'embedded' = $true;
            'filename' = 'Priceline-UK-Meta-Topline';
            'parameters' = @{
                'Advertiser' = 'Priceline+-+UK+-+Meta';
                'Product Category' = 'PPA'
            }
        },
        @{
            'worksheet' = 'CSV Download';
            'filetype' = 'csv';
            'attached' = $true;
            'embedded' = $false;
            'filename' = '#{$yesterday}-Intent-Media-Priceline-Data';
            'parameters' = @{
                'Advertiser' = 'Priceline-Ads,Priceline+-+UK,Priceline+-+US+-+Meta,Priceline+-+UK+-+Meta';
                'Product Category' = 'Total';
                'Auction Type Parameter' = 'Total'
            };
            'column_order' = @("Advertiser","Date","Local Currency","Product Category Type","Clicks","Impressions","Spend","Average Position","Spend in Local Currency","CPC in Local Currency","CPC","CTR","Auction Type")
        }

    );
    'entities' = @('Priceline-Ads')
}
