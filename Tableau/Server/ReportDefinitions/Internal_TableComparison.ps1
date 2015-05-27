$report = @{
    'frequency' = 'Daily';
    'subject' = 'Cross Table Validation as of #{$formatted_date}';
    'default_workbook' = 'Table Comparison';
    'assets' = @(
        @{
            'worksheet' = 'MetricsCrossvalidationAcrossTablesfiltered';
            'filetype' = 'png';
            'attached' = $false;
            'embedded' = $true;
            'filename' = '#{$yesterday}-Table-Comparison'
        }
    );
    'entities' = @('IM Cross Table Validation')
}