$report = @{
    'frequency' = 'Monday';
    'subject' = 'SSN Weekly Publisher Reports as of #{$formatted_date}';
    'default_workbook' = 'External SSN Dashboard';
    'assets' = @(
        @{
            'page_number' = 1;
            'worksheet' = 'Title Page';
            'filetype' = 'pdf';
            'page_layout' = 'landscape';
            'attached' = $true;
            'embedded' = $false;
            'filename' = '#{$yesterday}-Intent-Media-OWW-Publisher-Performance';
            'parameters' = @{
                'Channel Type' = 'OTA';
                'Publisher' = 'OWW';
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
            'filename' = '#{$yesterday}-Intent-Media-OWW-Publisher-Performance';
            'parameters' = @{
                'Channel Type' = 'OTA';
                'Publisher' = 'OWW';
                'Ad Unit Type' = 'Total'
            }
        },
        @{
            'page_number' = 3;
            'worksheet' = 'Gross Revenue Trend';
            'filetype' = 'pdf';
            'page_layout' = 'landscape';
            'attached' = $true;
            'embedded' = $false;
            'filename' = '#{$yesterday}-Intent-Media-OWW-Publisher-Performance';
            'parameters' = @{
                'Channel Type' = 'OTA';
                'Publisher' = 'OWW';
                'Ad Unit Type' = 'Total Hotel List Page'
            }
        },
        @{
            'page_number' = 4;
            'worksheet' = 'Gross Revenue Trend';
            'filetype' = 'pdf';
            'page_layout' = 'landscape';
            'attached' = $true;
            'embedded' = $false;
            'filename' = '#{$yesterday}-Intent-Media-OWW-Publisher-Performance';
            'parameters' = @{
                'Channel Type' = 'OTA';
                'Publisher' = 'OWW';
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
            'filename' = '#{$yesterday}-Intent-Media-OWW-Publisher-Performance';
            'parameters' = @{
                'Channel Type' = 'OTA';
                'Publisher' = 'OWW';
                'Ad Unit Type' = 'Total Hotel Details Page'
            }
        },
        @{
            'page_number' = 6;
            'worksheet' = 'Publisher Metrics';
            'filetype' = 'pdf';
            'attached' = $true;
            'embedded' = $false;
            'filename' = '#{$yesterday}-Intent-Media-OWW-Publisher-Performance';
            'parameters' = @{
                'Channel Type' = 'OTA';
                'Publisher' = 'OWW';
                'Ad Unit Type' = 'Total';
                'Breakdown' = 'Site'
            }
        },
        @{
            'page_number' = 7;
            'worksheet' = 'Publisher Metrics';
            'filetype' = 'pdf';
            'attached' = $true;
            'embedded' = $false;
            'filename' = '#{$yesterday}-Intent-Media-OWW-Publisher-Performance';
            'parameters' = @{
                'Channel Type' = 'OTA';
                'Publisher' = 'Orbitz';
                'Ad Unit Type' = 'Total';
                'Breakdown' = 'Page Type'
            }
        },
        @{
            'page_number' = 8;
            'worksheet' = 'Publisher Metrics';
            'filetype' = 'pdf';
            'attached' = $true;
            'embedded' = $false;
            'filename' = '#{$yesterday}-Intent-Media-OWW-Publisher-Performance';
            'parameters' = @{
                'Channel Type' = 'OTA';
                'Publisher' = 'CheapTickets';
                'Ad Unit Type' = 'Total';
                'Breakdown' = 'Ad Unit'
            }
        },
        @{
            'page_number' = 9;
            'worksheet' = 'Publisher Metrics';
            'filetype' = 'pdf';
            'attached' = $true;
            'embedded' = $false;
            'filename' = '#{$yesterday}-Intent-Media-OWW-Publisher-Performance';
            'parameters' = @{
                'Channel Type' = 'OTA';
                'Publisher' = 'ebookers';
                'Ad Unit Type' = 'Total';
                'Breakdown' = 'Ad Unit'
            }
        },
        @{
            'page_number' = 10;
            'worksheet' = 'Publisher Metrics';
            'filetype' = 'pdf';
            'attached' = $true;
            'embedded' = $false;
            'filename' = '#{$yesterday}-Intent-Media-OWW-Publisher-Performance';
            'parameters' = @{
                'Channel Type' = 'OTA';
                'Publisher' = 'HotelClub';
                'Ad Unit Type' = 'Total';
                'Breakdown' = 'Ad Unit'
            }
        },
        @{
            'page_number' = 11;
            'worksheet' = 'Publisher Metrics';
            'filetype' = 'pdf';
            'attached' = $true;
            'embedded' = $false;
            'filename' = '#{$yesterday}-Intent-Media-OWW-Publisher-Performance';
            'parameters' = @{
                'Channel Type' = 'OTA';
                'Publisher' = 'RatesToGo';
                'Ad Unit Type' = 'Total';
                'Breakdown' = 'Ad Unit'
            }
        },
        @{
            'page_number' = 12;
            'worksheet' = 'Publisher Funnel by Page';
            'filetype' = 'pdf';
            'attached' = $true;
            'embedded' = $false;
            'filename' = '#{$yesterday}-Intent-Media-OWW-Publisher-Performance';
            'parameters' = @{
                'Channel Type' = 'OTA';
                'Publisher' = 'OWW';
                'Ad Unit Type' = 'Total';
                'Breakdown' = 'Ad Unit'
            }
        },
        @{
            'page_number' = 13;
            'worksheet' = 'Key Performance Metrics by Market';
            'filetype' = 'pdf';
            'attached' = $true;
            'embedded' = $false;
            'filename' = '#{$yesterday}-Intent-Media-OWW-Publisher-Performance';
            'parameters' = @{
                'Channel Type' = 'OTA';
                'Publisher' = 'OWW';
                'Ad Unit Type' = 'Total'
            }
        },
        @{
            'page_number' = 14;
            'worksheet' = 'Performance by Region and Country';
            'filetype' = 'pdf';
            'attached' = $true;
            'embedded' = $false;
            'filename' = '#{$yesterday}-Intent-Media-OWW-Publisher-Performance';
            'parameters' = @{
                'Channel Type' = 'OTA';
                'Publisher' = 'OWW';
                'Ad Unit Type' = 'Total'
            }
        },
        @{
            'page_number' = 15;
            'worksheet' = 'Key Performance Metrics by Market';
            'filetype' = 'pdf';
            'attached' = $true;
            'embedded' = $false;
            'filename' = '#{$yesterday}-Intent-Media-OWW-Publisher-Performance';
            'parameters' = @{
                'Channel Type' = 'OTA';
                'Publisher' = 'Orbitz';
                'Ad Unit Type' = 'Total'
            }
        },
        @{
            'page_number' = 16;
            'worksheet' = 'Performance by Region and Country';
            'filetype' = 'pdf';
            'attached' = $true;
            'embedded' = $false;
            'filename' = '#{$yesterday}-Intent-Media-OWW-Publisher-Performance';
            'parameters' = @{
                'Channel Type' = 'OTA';
                'Publisher' = 'Orbitz';
                'Ad Unit Type' = 'Total'
            }
        },
        @{
            'page_number' = 17;
            'worksheet' = 'Key Performance Metrics by Market';
            'filetype' = 'pdf';
            'attached' = $true;
            'embedded' = $false;
            'filename' = '#{$yesterday}-Intent-Media-OWW-Publisher-Performance';
            'parameters' = @{
                'Channel Type' = 'OTA';
                'Publisher' = 'CheapTickets';
                'Ad Unit Type' = 'Total'
            }
        },
        @{
            'page_number' = 18;
            'worksheet' = 'Performance by Region and Country';
            'filetype' = 'pdf';
            'attached' = $true;
            'embedded' = $false;
            'filename' = '#{$yesterday}-Intent-Media-OWW-Publisher-Performance';
            'parameters' = @{
                'Channel Type' = 'OTA';
                'Publisher' = 'CheapTickets';
                'Ad Unit Type' = 'Total'
            }
        },
        @{
            'page_number' = 19;
            'worksheet' = 'Key Performance Metrics by Market';
            'filetype' = 'pdf';
            'attached' = $true;
            'embedded' = $false;
            'filename' = '#{$yesterday}-Intent-Media-OWW-Publisher-Performance';
            'parameters' = @{
                'Channel Type' = 'OTA';
                'Publisher' = 'ebookers';
                'Ad Unit Type' = 'Total'
            }
        },
        @{
            'page_number' = 20;
            'worksheet' = 'Performance by Region and Country';
            'filetype' = 'pdf';
            'attached' = $true;
            'embedded' = $false;
            'filename' = '#{$yesterday}-Intent-Media-OWW-Publisher-Performance';
            'parameters' = @{
                'Channel Type' = 'OTA';
                'Publisher' = 'ebookers';
                'Ad Unit Type' = 'Total'
            }
        },
        @{
            'page_number' = 21;
            'worksheet' = 'Key Performance Metrics by Market';
            'filetype' = 'pdf';
            'attached' = $true;
            'embedded' = $false;
            'filename' = '#{$yesterday}-Intent-Media-OWW-Publisher-Performance';
            'parameters' = @{
                'Channel Type' = 'OTA';
                'Publisher' = 'HotelClub';
                'Ad Unit Type' = 'Total'
            }
        },
        @{
            'page_number' = 22;
            'worksheet' = 'Performance by Region and Country';
            'filetype' = 'pdf';
            'attached' = $true;
            'embedded' = $false;
            'filename' = '#{$yesterday}-Intent-Media-OWW-Publisher-Performance';
            'parameters' = @{
                'Channel Type' = 'OTA';
                'Publisher' = 'HotelClub';
                'Ad Unit Type' = 'Total'
            }
        },
        @{
            'page_number' = 23;
            'worksheet' = 'Key Performance Metrics by Market';
            'filetype' = 'pdf';
            'attached' = $true;
            'embedded' = $false;
            'filename' = '#{$yesterday}-Intent-Media-OWW-Publisher-Performance';
            'parameters' = @{
                'Channel Type' = 'OTA';
                'Publisher' = 'RatesToGo';
                'Ad Unit Type' = 'Total'
            }
        },
        @{
            'page_number' = 24;
            'worksheet' = 'Performance by Region and Country';
            'filetype' = 'pdf';
            'attached' = $true;
            'embedded' = $false;
            'filename' = '#{$yesterday}-Intent-Media-OWW-Publisher-Performance';
            'parameters' = @{
                'Channel Type' = 'OTA';
                'Publisher' = 'RatesToGo';
                'Ad Unit Type' = 'Total'
            }
        },
		        @{
            'page_number' = 1;
            'worksheet' = 'Title Page';
            'filetype' = 'pdf';
            'page_layout' = 'landscape';
            'attached' = $true;
            'embedded' = $false;
            'filename' = '#{$yesterday}-Intent-Media-Travelport-Publisher-Performance';
            'parameters' = @{
                'Channel Type' = 'GDS';
                'Publisher' = 'Travelport';
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
            'filename' = '#{$yesterday}-Intent-Media-Travelport-Publisher-Performance';
            'parameters' = @{
                'Channel Type' = 'GDS';
                'Publisher' = 'Travelport';
                'Ad Unit Type' = 'Total'
            }
        },
        @{
            'page_number' = 3;
            'worksheet' = 'Publisher Metrics';
            'filetype' = 'pdf';
            'attached' = $true;
            'embedded' = $false;
            'filename' = '#{$yesterday}-Intent-Media-Travelport-Publisher-Performance';
            'parameters' = @{
                'Channel Type' = 'GDS';
                'Publisher' = 'Travelport';
                'Ad Unit Type' = 'Total';
                'Breakdown' = 'Site'
            }
        },
		@{
            'page_number' = 4;
            'worksheet' = 'Key Performance Metrics by GDS Market';
            'filetype' = 'pdf';
            'attached' = $true;
            'embedded' = $false;
            'filename' = '#{$yesterday}-Intent-Media-Travelport-Publisher-Performance';
            'parameters' = @{
                'Channel Type' = 'GDS';
                'Publisher' = 'Travelport';
                'Ad Unit Type' = 'Total'
            }
        },
        @{
            'page_number' = 5;
            'worksheet' = 'Performance by Region and Country';
            'filetype' = 'pdf';
            'attached' = $true;
            'embedded' = $false;
            'filename' = '#{$yesterday}-Intent-Media-Travelport-Publisher-Performance';
            'parameters' = @{
                'Channel Type' = 'GDS';
                'Publisher' = 'Travelport';
                'Ad Unit Type' = 'Total'
            }
        },
		@{
            'page_number' = 6;
            'worksheet' = 'Publisher Funnel by Page';
            'filetype' = 'pdf';
            'attached' = $true;
            'embedded' = $false;
            'filename' = '#{$yesterday}-Intent-Media-Travelport-Publisher-Performance';
            'parameters' = @{
                'Channel Type' = 'GDS';
                'Publisher' = 'Travelport';
                'Ad Unit Type' = 'Total';
                'Breakdown' = 'Site'
            }
        }
    );
    'entities' = @('IM Weekly Pub Reports and Rudan')
}