$report = @{
    'frequency' = 'Monday';
    'subject' = 'Publisher Revenue Tier Report of #{$formatted_date}';
    'default_workbook' = 'Publisher Revenue Tiers';
    'assets' = @(
        @{
            'page_number' = 1;
            'worksheet' = 'Table';
            'filetype' = 'png';
            'attached' = $false;
            'embedded' = $true;
            'filename' = '#{$yesterday}-Pub-Tier-Table'
        },
        @{
            'page_number' = 2;
            'worksheet' = 'Graph';
            'filetype' = 'png';
            'attached' = $false;
            'embedded' = $true;
            'filename' = '#{$yesterday}-Pub-Tier-Graph'
        }
    );
    'entities' = @('IM Pub')
}