$report = @{
    'frequency' = 'Daily';
    'subject' = 'Zero Beta Alert for #{$formatted_date}';
    'default_workbook' = 'Daily Zero Betas';
    'assets' = @(
        @{
            'worksheet' = 'FeatureList';
            'filetype' = 'png';
            'attached' = $false;
            'embedded' = $true;
            'filename' = '#{$yesterday}-Daily-Zero-Betas'
        }
    );
    'entities' = @('IM Data')
}