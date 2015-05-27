$report = @{
    'frequency' = 'Daily';
    'subject' = 'SSN Daily Dashboard Report as of #{$formatted_date}';
    'default_workbook' = 'SSN Dashboard';
    'assets' = @(
        @{
            'worksheet' = 'Gross Revenue Trend';
            'filetype' = 'png';
            'attached' = $false;
            'embedded' = $true;
            'filename' = '#{$yesterday}-SSN-Dashboard';
            'parameters' = @{
                'Publisher' = 'Total';
                'Ad Unit Type' = 'Total';
            }
        },
        @{
            'page_number' = 1;
            'worksheet' = 'Gross Revenue Trend';
            'filetype' = 'pdf';
            'page_layout' = 'landscape';
            'attached' = $true;
            'embedded' = $false;
            'filename' = '#{$yesterday}-SSN-Dashboard';
            'parameters' = @{
                'Channel Type' = 'Total';
                'Publisher' = 'Total';
                'Ad Unit Type' = 'Total'
            }
        },
        @{
            'page_number' = 2;
            'worksheet' = 'Gross Revenue Trend';
            'filetype' = 'pdf';
            'page_layout' = 'landscape';
            'attached' = $true;
            'embedded' = $false;
            'filename' = '#{$yesterday}-SSN-Dashboard';
            'parameters' = @{
                'Channel Type' = 'OTA';
                'Publisher' = 'Total';
                'Ad Unit Type' = 'Total Hotel List Page'
            }
        },
        @{
            'page_number' = 3;
            'worksheet' = 'Gross Revenue Trend';
            'filetype' = 'pdf';
            'page_layout' = 'landscape';
            'attached' = $true;
            'embedded' = $false;
            'filename' = '#{$yesterday}-SSN-Dashboard';
            'parameters' = @{
                'Channel Type' = 'OTA';
                'Publisher' = 'Total';
                'Ad Unit Type' = 'Total Hotel Details Page'
            }
        },
        @{
            'page_number' = 4;
            'worksheet' = 'Gross Revenue Trend';
            'filetype' = 'pdf';
            'page_layout' = 'landscape';
            'attached' = $true;
            'embedded' = $false;
            'filename' = '#{$yesterday}-SSN-Dashboard';
            'parameters' = @{
                'Channel Type' = 'OTA';
                'Publisher' = 'Total';
                'Ad Unit Type' = 'Total Packages List Page'
            }
        },
        @{
            'page_number' = 5;
            'worksheet' = 'Gross Revenue Trend';
            'filetype' = 'pdf';
            'page_layout' = 'landscape';
            'attached' = $true;
            'embedded' = $false;
            'filename' = '#{$yesterday}-SSN-Dashboard';
            'parameters' = @{
                'Channel Type' = 'GDS';
                'Publisher' = 'Total';
                'Ad Unit Type' = 'Total'
            }
        },
        @{
            'page_number' = 6;
            'workbook' = 'SSN Waterfall Charts';
            'worksheet' = 'Waterfall Charts';
            'filetype' = 'pdf';
            'attached' = $true;
            'embedded' = $false;
            'filename' = '#{$yesterday}-SSN-Dashboard';
        },
        @{
            'page_number' = 7;
            'worksheet' = 'Publisher Metrics';
            'filetype' = 'pdf';
            'attached' = $true;
            'embedded' = $false;
            'filename' = '#{$yesterday}-SSN-Dashboard';
            'parameters' = @{
                'Channel Type' = 'Total';
                'Publisher' = 'Total';
                'Ad Unit Type' = 'Total';
                'Breakdown' = 'Site'
            }
        }, 
        @{
            'page_number' = 8;
            'worksheet' = 'Publisher Metrics';
            'filetype' = 'pdf';
            'attached' = $true;
            'embedded' = $false;
            'filename' = '#{$yesterday}-SSN-Dashboard';
            'parameters' = @{
                'Channel Type' = 'Total';
                'Publisher' = 'OWW';
                'Ad Unit Type' = 'Total';
                'Breakdown' = 'Page Type'
            }
        }, 
        @{
            'page_number' = 9;
            'worksheet' = 'Advertiser Metrics';
            'filetype' = 'pdf';
            'attached' = $true;
            'embedded' = $false;
            'filename' = '#{$yesterday}-SSN-Dashboard';
            'parameters' = @{
                'Channel Type' = 'Total';
                'Publisher' = 'Total';
                'Ad Unit Type' = 'Total';
                'Breakdown' = 'Site'
            }
        }, 
        @{
            'page_number' = 10;
            'worksheet' = 'Variance by Market';
            'filetype' = 'pdf';
            'attached' = $true;
            'embedded' = $false;
            'filename' = '#{$yesterday}-SSN-Dashboard';
            'parameters' = @{
                'Channel Type' = 'Total';
                'Publisher' = 'Total';
                'Ad Unit Type' = 'Total'
            }
        }, 
        @{
            'page_number' = 11;
            'worksheet' = 'Key Performance Metrics by Market';
            'filetype' = 'pdf';
            'attached' = $true;
            'embedded' = $false;
            'filename' = '#{$yesterday}-SSN-Dashboard';
            'parameters' = @{
                'Channel Type' = 'Total';
                'Publisher' = 'Total';
                'Ad Unit Type' = 'Total';
                'Aggregated By' = 'Week'
            }
        }
    );
    'entities' = @('IM Everyone')
}