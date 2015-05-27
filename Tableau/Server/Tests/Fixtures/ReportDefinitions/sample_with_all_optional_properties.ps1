$report = @{
    'frequency' = 'Daily';
    'subject' = 'Intent Media AfT Placement Data Report';
    'sender' = 'aft-reports@intentmedia.com'; 
    'recipient_cc' = ''
    'recipient_bcc' = ''
    'default_workbook' = 'External AfT Placement Report';
    'static_image' = 'im_ads_for_travel.png';
    'assets' = @{
        'publisher_data_page_1' = @{
            'page_number' = 1;
            'workbook' = 'External AfT Placement Report';
            'worksheet' = 'Data Without Suppression Reasons';
            'filetype' = 'csv';
            'attached' = $true;
            'embedded' = $false;
            'filename' = 'publisher_data_csv';
            'parameters' = @{
                'Publisher' = 'Air Fast Tickets';
                'Time' = 'Day'
            };
            'column_order' = @('asfd','afd')
        };
        'publisher_data_page_2' = @{
            'page_number' = 2;
            'workbook' = 'External AfT Placement Report';
            'worksheet' = 'Data Without Suppression Reasons';
            'filetype' = 'csv';
            'attached' = $true;
            'embedded' = $false;
            'filename' = 'publisher_data_csv';
            'parameters' = @{
                'Publisher' = 'CheapTickets';
                'Time' = 'Day'
            };
            'column_order' = @('asfd','afd')
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