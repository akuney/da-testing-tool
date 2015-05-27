$report = @{
    'frequency' = 'Daily';
    'subject' = 'SCA Advertiser Report as of $formatted_date';
    'default_workbook' = 'SCA Advertiser Report';
    'assets' = @(
        @{
            'page_number' = 1;
            'worksheet' = 'Advertiser Tornados';
            'filetype' = 'png';
            'attached' = $false;
            'embedded' = $true;
            'filename' = '#{$yesterday}-Advertiser-Tornados';
        },
        @{
            'page_number' = 2;
            'worksheet' = 'Advertiser Data Table';
            'filetype' = 'png';
            'attached' = $false;
            'embedded' = $true;
            'filename' = '#{$yesterday}-Advertiser-Data-Table';
        }
    );
    'entities' = @('IM AfT Ads')
}
