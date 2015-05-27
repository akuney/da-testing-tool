$report = @{
    'frequency' = 'Daily';
    'subject' = 'SCA Model Metrics Report as of #{$formatted_date}';
    'default_workbook' = 'SCA Model Metrics Dashboard';
    'assets' = @(
        @{
            'worksheet' = 'SCA Model Metrics Email';
            'filetype' = 'png';
            'attached' = $false;
            'embedded' = $true;
            'filename' = '#{$yesterday}-Model-Metrics'
        }
    );
    'entities' = @('IM Data')
}
