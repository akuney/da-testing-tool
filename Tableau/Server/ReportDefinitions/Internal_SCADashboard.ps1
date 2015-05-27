$report = @{
    'frequency' = 'Daily';
    'subject' = 'SCA Daily Dashboard Report as of #{$formatted_date}';
    'default_workbook' = 'SCA Multiproduct Dashboard';
    'assets' = @(
        @{
            'page_number' = 1;
            'worksheet' = 'Gross Revenue Trend';
            'filetype' = 'png';
            'attached' = $false;
            'embedded' = $true;
            'filename' = '#{$yesterday}-SCA-Flights-Dashboard';
            'parameters' = @{
                'Product Category' = 'Flights';
                'Ad Unit Type' = 'Total';
            }
        },
        @{
            'page_number' = 2;
            'worksheet' = 'Gross Revenue Trend';
            'filetype' = 'png';
            'attached' = $false;
            'embedded' = $true;
            'filename' = '#{$yesterday}-SCA-Hotels-Dashboard';
            'parameters' = @{
                'Product Category' = 'Hotels';
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
            'filename' = '#{$yesterday}-SCA-Flights-Dashboard';
            'parameters' = @{
                'Product Category' = 'Flights';
                'Ad Unit Type' = 'Total';
            }
        },
        @{
            'page_number' = 2;
            'worksheet' = 'Performance Benchmarks - Site';
            'filetype' = 'pdf';
            'attached' = $true;
            'embedded' = $false;
            'filename' = '#{$yesterday}-SCA-Flights-Dashboard';
            'parameters' = @{
                'Product Category' = 'Flights';
                'Ad Unit Type' = 'Total';
                'Aggregated By' = 'Week';
                'Breakdown' = 'Publisher Tier';
            }
        },
        @{
            'page_number' = 3;
            'worksheet' = 'Key Performance Metrics by Year';
            'filetype' = 'pdf';
            'attached' = $true;
            'embedded' = $false;
            'filename' = '#{$yesterday}-SCA-Flights-Dashboard';
            'parameters' = @{
                'Product Category' = 'Flights';
                'Ad Unit Type' = 'Total';
            }
        },
        @{
            'page_number' = 4;
            'worksheet' = 'Gross Revenue Trend';
            'filetype' = 'pdf';
            'page_layout' = 'landscape';
            'attached' = $true;
            'embedded' = $false;
            'filename' = '#{$yesterday}-SCA-Flights-Dashboard';
            'parameters' = @{
                'Product Category' = 'Flights';
                'Ad Unit Type' = 'List page (web)';
            }
        },
        @{
            'page_number' = 5;
            'worksheet' = 'Performance Benchmarks - Site';
            'filetype' = 'pdf';
            'attached' = $true;
            'embedded' = $false;
            'filename' = '#{$yesterday}-SCA-Flights-Dashboard';
            'parameters' = @{
                'Product Category' = 'Flights';
                'Ad Unit Type' = 'List page (web)';
                'Aggregated By' = 'Week';
                'Breakdown' = 'Publisher Tier';
            }
        },
        @{
            'page_number' = 6;
            'worksheet' = 'Key Performance Metrics by Year';
            'filetype' = 'pdf';
            'attached' = $true;
            'embedded' = $false;
            'filename' = '#{$yesterday}-SCA-Flights-Dashboard';
            'parameters' = @{
                'Product Category' = 'Flights';
                'Ad Unit Type' = 'List page (web)';
                'Placement' = 'Total';
            }
        },
        @{
            'page_number' = 7;
            'worksheet' = 'Gross Revenue Trend';
            'filetype' = 'pdf';
            'page_layout' = 'landscape';
            'attached' = $true;
            'embedded' = $false;
            'filename' = '#{$yesterday}-SCA-Flights-Dashboard';
            'parameters' = @{
                'Product Category' = 'Flights';
                'Ad Unit Type' = 'Exit unit';
                'Placement' = 'Total';
            }
        },
        @{
            'page_number' = 8;
            'worksheet' = 'Performance Benchmarks - Site';
            'filetype' = 'pdf';
            'attached' = $true;
            'embedded' = $false;
            'filename' = '#{$yesterday}-SCA-Flights-Dashboard';
            'parameters' = @{
                'Product Category' = 'Flights';
                'Ad Unit Type' = 'Exit unit';
                'Aggregated By' = 'Week';
                'Breakdown' = 'Publisher Tier';
            }
        },
        @{
            'page_number' = 9;
            'worksheet' = 'Key Performance Metrics by Year';
            'filetype' = 'pdf';
            'attached' = $true;
            'embedded' = $false;
            'filename' = '#{$yesterday}-SCA-Flights-Dashboard';
            'parameters' = @{
                'Product Category' = 'Flights';
                'Ad Unit Type' = 'Exit unit';
            }
        },
        @{
            'page_number' = 10;
            'worksheet' = 'Detailed Funnels by Ad Unit';
            'filetype' = 'pdf';
            'attached' = $true;
            'embedded' = $false;
            'filename' = '#{$yesterday}-SCA-Flights-Dashboard';
            'parameters' = @{
                'Product Category' = 'Flights';
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
            'filename' = '#{$yesterday}-SCA-Hotels-Dashboard';
            'parameters' = @{
                'Product Category' = 'Hotels';
                'Ad Unit Type' = 'Total';
            }
        },
        @{
            'page_number' = 2;
            'worksheet' = 'Performance Benchmarks - Site';
            'filetype' = 'pdf';
            'attached' = $true;
            'embedded' = $false;
            'filename' = '#{$yesterday}-SCA-Hotels-Dashboard';
            'parameters' = @{
                'Product Category' = 'Hotels';
                'Ad Unit Type' = 'Total';
                'Aggregated By' = 'Week';
                'Breakdown' = 'Publisher Tier';
            }
        },
        @{
            'page_number' = 3;
            'worksheet' = 'Key Performance Metrics by Year';
            'filetype' = 'pdf';
            'attached' = $true;
            'embedded' = $false;
            'filename' = '#{$yesterday}-SCA-Hotels-Dashboard';
            'parameters' = @{
                'Product Category' = 'Hotels';
                'Ad Unit Type' = 'Total';
            }
        },
        @{
            'page_number' = 4;
            'worksheet' = 'Gross Revenue Trend';
            'filetype' = 'pdf';
            'page_layout' = 'landscape';
            'attached' = $true;
            'embedded' = $false;
            'filename' = '#{$yesterday}-SCA-Hotels-Dashboard';
            'parameters' = @{
                'Product Category' = 'Hotels';
                'Ad Unit Type' = 'List page (web)';
            }
        },
        @{
            'page_number' = 5;
            'worksheet' = 'Performance Benchmarks - Site';
            'filetype' = 'pdf';
            'attached' = $true;
            'embedded' = $false;
            'filename' = '#{$yesterday}-SCA-Hotels-Dashboard';
            'parameters' = @{
                'Product Category' = 'Hotels';
                'Ad Unit Type' = 'List page (web)';
                'Aggregated By' = 'Week';
                'Breakdown' = 'Publisher Tier';
            }
        },
        @{
            'page_number' = 6;
            'worksheet' = 'Key Performance Metrics by Year';
            'filetype' = 'pdf';
            'attached' = $true;
            'embedded' = $false;
            'filename' = '#{$yesterday}-SCA-Hotels-Dashboard';
            'parameters' = @{
                'Product Category' = 'Hotels';
                'Ad Unit Type' = 'List page (web)';
                'Placement' = 'Total';
            }
        },
        @{
            'page_number' = 7;
            'worksheet' = 'Gross Revenue Trend';
            'filetype' = 'pdf';
            'page_layout' = 'landscape';
            'attached' = $true;
            'embedded' = $false;
            'filename' = '#{$yesterday}-SCA-Hotels-Dashboard';
            'parameters' = @{
                'Product Category' = 'Hotels';
                'Ad Unit Type' = 'Exit unit';
                'Placement' = 'Total';
            }
        },
        @{
            'page_number' = 8;
            'worksheet' = 'Performance Benchmarks - Site';
            'filetype' = 'pdf';
            'attached' = $true;
            'embedded' = $false;
            'filename' = '#{$yesterday}-SCA-Hotels-Dashboard';
            'parameters' = @{
                'Product Category' = 'Hotels';
                'Ad Unit Type' = 'Exit unit';
                'Aggregated By' = 'Week';
                'Breakdown' = 'Publisher Tier';
            }
        },
        @{
            'page_number' = 9;
            'worksheet' = 'Key Performance Metrics by Year';
            'filetype' = 'pdf';
            'attached' = $true;
            'embedded' = $false;
            'filename' = '#{$yesterday}-SCA-Hotels-Dashboard';
            'parameters' = @{
                'Product Category' = 'Hotels';
                'Ad Unit Type' = 'Exit unit';
            }
        },
        @{
            'page_number' = 10;
            'worksheet' = 'Detailed Funnels by Ad Unit';
            'filetype' = 'pdf';
            'attached' = $true;
            'embedded' = $false;
            'filename' = '#{$yesterday}-SCA-Hotels-Dashboard';
            'parameters' = @{
                'Product Category' = 'Hotels';
                'Ad Unit Type' = 'Total';
            }
        }
     );
    'entities' = @('IM Everyone')
}