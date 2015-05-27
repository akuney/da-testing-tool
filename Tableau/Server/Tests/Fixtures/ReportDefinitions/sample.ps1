$report = @{
    'frequency' = 'Daily';
    'subject' = 'sample_subject';
    'default_workbook' = 'sample_workbook';
    'assets' = @{
        'publisher_data' = @{
            'worksheet' = 'sample_worksheet';
            'filetype' = 'csv';
            'attached' = $true;
            'embedded' = $false;
            'filename' = 'sample_filename';
            'parameters' = @{
                'sample_parameter_key' = 'sample_parameter_value'
            }
        }
    };
   'entities' = @('sample_entity')
}