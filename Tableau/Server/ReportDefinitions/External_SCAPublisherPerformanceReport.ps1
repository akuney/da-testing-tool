$report = @{
    'frequency' = 'Daily';
    'subject' = 'Intent Media #{$entity} SCA Performance as of #{$formatted_date}';
    'default_workbook' = 'External SCA Multiproduct Dashboard';
    'assets' = @(
        @{
            'worksheet' = 'SCA CSV';
            'filetype' = 'csv';
            'attached' = $true;
            'embedded' = $false;
            'filename' = '#{$yesterday}-Intent-Media-#{$entity}-Publisher-Performance';
            'parameters' = @{
                'Publisher' = '#{$loop_through_parameter}';
                'Product Category' = 'Total'
            };
            'column_order' = @('Ad Unit', 'Date', 'Month', 'Product Category Type', 'Quarter', 'Site', 'Type of Ad Unit', 'Week End Date', 'Year Number', 'Addressable Page Views', 'Clicks', 'Gross Media Revenue', 'Interactions', 'Net Media Revenue', 'Page Views', 'Pages Served')
        }
    );
    'entities' = @{
        'Bookit' = @('Bookit');
        'Fareportal' = @('CheapOair','OneTravel', 'CheapOair UK');
        'Gogobot' = @('Gogobot');
        'Insanely Cheap Flights' = @('Insanely Cheap Flights');
        'KAYAK' = @('KAYAK','KAYAK UK', 'KAYAK CA');
        'Odigeo' = @('Opodo UK');
        'Travelzoo Inc.' = @('Fly.com','Travelzoo','Fly.com UK')
    }
}
