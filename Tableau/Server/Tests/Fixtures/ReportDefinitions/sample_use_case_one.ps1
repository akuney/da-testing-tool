$report = @{
    'frequency' = 'Daily';
    'subject' = 'Intent Media AfT Placement Data Report';
    'default_workbook' = 'External AfT Placement Report';
    'assets' = @{
        'publisher_data_page_1' = @{
            'worksheet' = 'Data Without Suppression Reasons';
            'filetype' = 'csv';
            'attached' = $true;
            'embedded' = $false;
            'filename' = 'publisher_data_csv';
            'parameters' = @{
                'Publisher' = 'Air Fast Tickets';
                'Time' = 'Day'
            }
        };
        'publisher_data_page_2' = @{
            'worksheet' = 'Data Without Suppression Reasons';
            'filetype' = 'csv';
            'attached' = $true;
            'embedded' = $false;
            'filename' = 'publisher_data_csv';
            'parameters' = @{
                'Publisher' = 'CheapTickets';
                'Time' = 'Day'
            }
        };
        'advertiser_data' = @{
            'workbook' = 'External AfT Advertiser Report for Advertisers';
            'worksheet' = 'Topline';
            'filetype' = 'png';
            'attached' = $false;
            'embedded' = $true;
            'filename' = 'topline';
            'parameters' = @{
                'Advertiser' = 'Expedia-Ads'
            }
        }
    };
    'entities' = @('Intent Media')
}