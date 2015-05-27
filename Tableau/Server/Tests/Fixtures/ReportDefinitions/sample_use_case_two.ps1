$report = @{
    'frequency' = 'Daily';
    'subject' = 'Intent Media #{$entity} AfT Placement Data Report';
    'default_workbook' = 'External AfT Placement Report';
    'assets' = @{
        'publisher_data' = @{
            'worksheet' = 'Data Without Suppression Reasons';
            'filetype' = 'csv';
            'attached' = $true;
            'embedded' = $false;
            'filename' = '#{$entity}_placement_csv';
            'parameters' = @{
                'Publisher' = '#{$entity}';
                'Time' = 'Day'
            }
        }
    };
    'entities' = @('Air Fast Tickets', 'Amoma')
}