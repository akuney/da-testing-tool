$report = @{
    'frequency' = 'Daily';
    'subject' = 'TBD - #{$subject_line}';
    'default_workbook' = 'Total Daily Gross Revenue';
    'assets' = @(
        @{
            'worksheet' = 'Subject';
            'filetype' = 'csv';
            'attached' = $false;
            'embedded' = $false;
            'filename' = '#{$yesterday}-Total-Gross-Revenue'
        }
    );
    'entities' = @('IM Everyone')
}