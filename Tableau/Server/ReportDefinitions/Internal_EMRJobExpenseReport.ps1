$report = @{
    'frequency' = 'Monthly';
    'subject' = 'EMR Job Expense Dashboard as of $formatted_date';
    'default_workbook' = 'EMR Job Expense Dashboard';
    'assets' = @(
        @{
            'page_number' = 1;
            'worksheet' = 'By Month';
            'filetype' = 'png';
            'attached' = $false;
            'embedded' = $true;
            'filename' = '#{$yesterday}-By-Month';
        },
        @{
            'page_number' = 2;
            'worksheet' = 'By Job';
            'filetype' = 'png';
            'attached' = $false;
            'embedded' = $true;
            'filename' = '#{$yesterday}-By-Job';
        }
    );
    'entities' = @('IM Tech and Product')
}
