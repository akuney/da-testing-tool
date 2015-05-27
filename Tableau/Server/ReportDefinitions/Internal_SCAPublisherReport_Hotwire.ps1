$report = @{
    'frequency' = 'Monday';
    'subject' = 'SCA Weekly Hotwire  Report as of #{$formatted_date}';
    'default_workbook' = 'External SCA Multiproduct Dashboard';
    'assets' = @(
        @{
            'page_number' = 1;
            'worksheet' = 'Title Page';
            'filetype' = 'pdf';
            'page_layout' = 'landscape';
            'attached' = $true;
            'embedded' = $false;
            'filename' = '#{$yesterday}-Intent-Media-Hotwire-Flights-Performance';
            'parameters' = @{
                'Publisher' = 'Hotwire';
                'Product Category' = 'Flights';
                'Ad Unit Type' = 'Total';
                'Page Number' = '1'
            }
        },
        @{
            'page_number' = 2;
            'worksheet' = 'Gross Revenue Trend - No Segmentation';
            'filetype' = 'pdf';
            'page_layout' = 'landscape';
            'attached' = $true;
            'embedded' = $false;
            'filename' = '#{$yesterday}-Intent-Media-Hotwire-Flights-Performance';
            'parameters' = @{
                'Publisher' = 'Hotwire';
                'Product Category' = 'Flights';
                'Ad Unit Type' = 'Total';
                'Page Number' = '2'
            }
        },
        @{
            'page_number' = 3;
            'worksheet' = 'Gross Revenue Trend - No Segmentation';
            'filetype' = 'pdf';
            'page_layout' = 'landscape';
            'attached' = $true;
            'embedded' = $false;
            'filename' = '#{$yesterday}-Intent-Media-Hotwire-Flights-Performance';
            'parameters' = @{
                'Publisher' = 'Hotwire';
                'Product Category' = 'Flights';
                'Ad Unit Type' = 'Total List Page';
                'Page Number' = '3'
            }
        },
        @{
            'page_number' = 4;
            'worksheet' = 'Gross Revenue Trend - No Segmentation';
            'filetype' = 'pdf';
            'page_layout' = 'landscape';
            'attached' = $true;
            'embedded' = $false;
            'filename' = '#{$yesterday}-Intent-Media-Hotwire-Flights-Performance';
            'parameters' = @{
                'Publisher' = 'Hotwire';
                'Product Category' = 'Flights';
                'Ad Unit Type' = 'Total Media Fill In';
                'Page Number' = '4'
            }
        },
        @{
            'page_number' = 5;
            'worksheet' = 'Gross Revenue Trend - No Segmentation';
            'filetype' = 'pdf';
            'page_layout' = 'landscape';
            'attached' = $true;
            'embedded' = $false;
            'filename' = '#{$yesterday}-Intent-Media-Hotwire-Flights-Performance';
            'parameters' = @{
                'Publisher' = 'Hotwire';
                'Product Category' = 'Flights';
                'Ad Unit Type' = 'Total Exit Units';
                'Page Number' = '5'
            }
        },
        @{
            'page_number' = 6;
            'worksheet' = 'Key Performance Metrics I';
            'filetype' = 'pdf';
            'attached' = $true;
            'embedded' = $false;
            'filename' = '#{$yesterday}-Intent-Media-Hotwire-Flights-Performance';
            'parameters' = @{
                'Publisher' = 'Hotwire';
                'Product Category' = 'Flights';
                'Ad Unit Type' = 'Total';
                'Breakdown' = 'Site';
                'Page Number' = '6'
            }
        },
        @{
            'page_number' = 7;
            'worksheet' = 'Key Performance Metrics II';
            'filetype' = 'pdf';
            'attached' = $true;
            'embedded' = $false;
            'filename' = '#{$yesterday}-Intent-Media-Hotwire-Flights-Performance';
            'parameters' = @{
                'Publisher' = 'Hotwire';
                'Product Category' = 'Flights';
                'Ad Unit Type' = 'Total';
                'Breakdown' = 'Site';
                'Page Number' = '7'
            }
        },
        @{
            'page_number' = 8;
            'worksheet' = 'Detailed Funnels by Ad Unit I';
            'filetype' = 'pdf';
            'page_layout' = 'landscape';
            'attached' = $true;
            'embedded' = $false;
            'filename' = '#{$yesterday}-Intent-Media-Hotwire-Flights-Performance';
            'parameters' = @{
                'Publisher' = 'Hotwire';
                'Product Category' = 'Flights';
                'Ad Unit Type' = 'Total';
                'Breakdown' = 'Ad Unit';
                'Page Number' = '8'
            }
        },
        @{
            'page_number' = 9;
            'worksheet' = 'Detailed Funnels by Ad Unit II';
            'filetype' = 'pdf';
            'page_layout' = 'landscape';
            'attached' = $true;
            'embedded' = $false;
            'filename' = '#{$yesterday}-Intent-Media-Hotwire-Flights-Performance';
            'parameters' = @{
                'Publisher' = 'Hotwire';
                'Product Category' = 'Flights';
                'Ad Unit Type' = 'Total';
                'Breakdown' = 'Ad Unit';
                'Page Number' = '9'
            }
        },
        @{
            'page_number' = 1;
            'worksheet' = 'Title Page';
            'filetype' = 'pdf';
            'page_layout' = 'landscape';
            'attached' = $true;
            'embedded' = $false;
            'filename' = '#{$yesterday}-Intent-Media-Hotwire-Hotels-Performance';
            'parameters' = @{
                'Publisher' = 'Hotwire';
                'Product Category' = 'Hotels';
                'Ad Unit Type' = 'Total';
                'Page Number' = '1'
            }
        },
        @{
            'page_number' = 2;
            'worksheet' = 'Gross Revenue Trend - No Segmentation';
            'filetype' = 'pdf';
            'page_layout' = 'landscape';
            'attached' = $true;
            'embedded' = $false;
            'filename' = '#{$yesterday}-Intent-Media-Hotwire-Hotels-Performance';
            'parameters' = @{
                'Publisher' = 'Hotwire';
                'Product Category' = 'Hotels';
                'Ad Unit Type' = 'Total';
                'Page Number' = '2'
            }
        },
        @{
            'page_number' = 3;
            'worksheet' = 'Gross Revenue Trend - No Segmentation';
            'filetype' = 'pdf';
            'page_layout' = 'landscape';
            'attached' = $true;
            'embedded' = $false;
            'filename' = '#{$yesterday}-Intent-Media-Hotwire-Hotels-Performance';
            'parameters' = @{
                'Publisher' = 'Hotwire';
                'Product Category' = 'Hotels';
                'Ad Unit Type' = 'Total List Page';
                'Page Number' = '3'
            }
        },
        @{
            'page_number' = 4;
            'worksheet' = 'Gross Revenue Trend - No Segmentation';
            'filetype' = 'pdf';
            'page_layout' = 'landscape';
            'attached' = $true;
            'embedded' = $false;
            'filename' = '#{$yesterday}-Intent-Media-Hotwire-Hotels-Performance';
            'parameters' = @{
                'Publisher' = 'Hotwire';
                'Product Category' = 'Hotels';
                'Ad Unit Type' = 'Total Exit Units';
                'Page Number' = '4'
            }
        },
        @{
            'page_number' = 5;
            'worksheet' = 'Key Performance Metrics I';
            'filetype' = 'pdf';
            'attached' = $true;
            'embedded' = $false;
            'filename' = '#{$yesterday}-Intent-Media-Hotwire-Hotels-Performance';
            'parameters' = @{
                'Publisher' = 'Hotwire';
                'Product Category' = 'Hotels';
                'Ad Unit Type' = 'Total';
                'Breakdown' = 'Site';
                'Page Number' = '5'
            }
        },
        @{
            'page_number' = 6;
            'worksheet' = 'Key Performance Metrics II';
            'filetype' = 'pdf';
            'attached' = $true;
            'embedded' = $false;
            'filename' = '#{$yesterday}-Intent-Media-Hotwire-Hotels-Performance';
            'parameters' = @{
                'Publisher' = 'Hotwire';
                'Product Category' = 'Hotels';
                'Ad Unit Type' = 'Total';
                'Breakdown' = 'Site';
                'Page Number' = '6'
            }
        },
        @{
            'page_number' = 7;
            'worksheet' = 'Detailed Funnels by Ad Unit I';
            'filetype' = 'pdf';
            'page_layout' = 'landscape';
            'attached' = $true;
            'embedded' = $false;
            'filename' = '#{$yesterday}-Intent-Media-Hotwire-Hotels-Performance';
            'parameters' = @{
                'Publisher' = 'Hotwire';
                'Product Category' = 'Hotels';
                'Ad Unit Type' = 'Total';
                'Breakdown' = 'Ad Unit';
                'Page Number' = '7'
            }
        },
        @{
            'page_number' = 8;
            'worksheet' = 'Detailed Funnels by Ad Unit II';
            'filetype' = 'pdf';
            'page_layout' = 'landscape';
            'attached' = $true;
            'embedded' = $false;
            'filename' = '#{$yesterday}-Intent-Media-Hotwire-Hotels-Performance';
            'parameters' = @{
                'Publisher' = 'Hotwire';
                'Product Category' = 'Hotels';
                'Ad Unit Type' = 'Total';
                'Breakdown' = 'Ad Unit';
                'Page Number' = '8'
            }
        }
    );
    'entities' = @('IM Weekly Pub Reports')
}