$report = @{
    'frequency' = 'Daily';
    'subject' = 'Metrics Exception Report as of #{$formatted_date}';
    'default_workbook' = 'External Exception Reporting Dashboard';
    'assets' = @(
        @{
            'worksheet' = 'AllMetrics';
            'filetype' = 'csv';
            'attached' = $true;
            'embedded' = $false;
            'filename' = '#{$yesterday}-metric-exception-report';
            'parameters' = @{
                 'Measure' = 'Ad Calls';
                 'Breakdown' = 'Site';
                 'Standard Deviations' = '2';
                 'Drop MinMax' = 'true'
            };
            'column_order' = @('Date','Breakdown','Ad Type, Product, Site, Ad Unit, Browser Family','Measure','Metric','Metric Average','Metric Z-Score','Revenue Impact')
        },
        @{
            'worksheet' = 'AllMetrics';
            'filetype' = 'csv';
            'attached' = $true;
            'embedded' = $false;
            'filename' = '#{$yesterday}-metric-exception-report';
            'parameters' = @{
                 'Measure' = 'Percent Served';
                 'Breakdown' = 'Site';
                 'Standard Deviations' = '2';
                 'Drop MinMax' = 'true'
            };
            'column_order' = @('Date','Breakdown','Ad Type, Product, Site, Ad Unit, Browser Family','Measure','Metric','Metric Average','Metric Z-Score','Revenue Impact')
        },
        @{
            'worksheet' = 'AllMetrics';
            'filetype' = 'csv';
            'attached' = $true;
            'embedded' = $false;
            'filename' = '#{$yesterday}-metric-exception-report';
            'parameters' = @{
                 'Measure' = 'Interaction Rate';
                 'Breakdown' = 'Site';
                 'Standard Deviations' = '2';
                 'Drop MinMax' = 'true'
            };
            'column_order' = @('Date','Breakdown','Ad Type, Product, Site, Ad Unit, Browser Family','Measure','Metric','Metric Average','Metric Z-Score','Revenue Impact')
        },
        @{
            'worksheet' = 'AllMetrics';
            'filetype' = 'csv';
            'attached' = $true;
            'embedded' = $false;
            'filename' = '#{$yesterday}-metric-exception-report';
            'parameters' = @{
                 'Measure' = 'CPI';
                 'Breakdown' = 'Site';
                 'Standard Deviations' = '2';
                 'Drop MinMax' = 'false'
            };
            'column_order' = @('Date','Breakdown','Ad Type, Product, Site, Ad Unit, Browser Family','Measure','Metric','Metric Average','Metric Z-Score','Revenue Impact')
        },
        @{
            'worksheet' = 'AllMetrics';
            'filetype' = 'csv';
            'attached' = $true;
            'embedded' = $false;
            'filename' = '#{$yesterday}-metric-exception-report';
            'parameters' = @{
                 'Measure' = 'Gross Media Revenue';
                 'Breakdown' = 'Site';
                 'Standard Deviations' = '2';
                 'Drop MinMax' = 'true'
            };
            'column_order' = @('Date','Breakdown','Ad Type, Product, Site, Ad Unit, Browser Family','Measure','Metric','Metric Average','Metric Z-Score','Revenue Impact')
        },
        @{
            'worksheet' = 'AllMetrics';
            'filetype' = 'csv';
            'attached' = $true;
            'embedded' = $false;
            'filename' = '#{$yesterday}-metric-exception-report';
            'parameters' = @{
                 'Measure' = 'Ad Calls';
                 'Breakdown' = 'Ad Unit';
                 'Standard Deviations' = '2';
                 'Drop MinMax' = 'true'
            };
            'column_order' = @('Date','Breakdown','Ad Type, Product, Site, Ad Unit, Browser Family','Measure','Metric','Metric Average','Metric Z-Score','Revenue Impact')
        },
        @{
            'worksheet' = 'AllMetrics';
            'filetype' = 'csv';
            'attached' = $true;
            'embedded' = $false;
            'filename' = '#{$yesterday}-metric-exception-report';
            'parameters' = @{
                 'Measure' = 'Percent Served';
                 'Breakdown' = 'Ad Unit';
                 'Standard Deviations' = '2';
                 'Drop MinMax' = 'true'
            };
            'column_order' = @('Date','Breakdown','Ad Type, Product, Site, Ad Unit, Browser Family','Measure','Metric','Metric Average','Metric Z-Score','Revenue Impact')
        },
        @{
            'worksheet' = 'AllMetrics';
            'filetype' = 'csv';
            'attached' = $true;
            'embedded' = $false;
            'filename' = '#{$yesterday}-metric-exception-report';
            'parameters' = @{
                 'Measure' = 'Interaction Rate';
                 'Breakdown' = 'Ad Unit';
                 'Standard Deviations' = '2';
                 'Drop MinMax' = 'true'
            };
            'column_order' = @('Date','Breakdown','Ad Type, Product, Site, Ad Unit, Browser Family','Measure','Metric','Metric Average','Metric Z-Score','Revenue Impact')
        },
        @{
            'worksheet' = 'AllMetrics';
            'filetype' = 'csv';
            'attached' = $true;
            'embedded' = $false;
            'filename' = '#{$yesterday}-metric-exception-report';
            'parameters' = @{
                 'Measure' = 'CPI';
                 'Breakdown' = 'Ad Unit';
                 'Standard Deviations' = '2';
                 'Drop MinMax' = 'false'
            };
            'column_order' = @('Date','Breakdown','Ad Type, Product, Site, Ad Unit, Browser Family','Measure','Metric','Metric Average','Metric Z-Score','Revenue Impact')
        },
        @{
            'worksheet' = 'AllMetrics';
            'filetype' = 'csv';
            'attached' = $true;
            'embedded' = $false;
            'filename' = '#{$yesterday}-metric-exception-report';
            'parameters' = @{
                 'Measure' = 'Gross Media Revenue';
                 'Breakdown' = 'Ad Unit';
                 'Standard Deviations' = '2';
                 'Drop MinMax' = 'true'
            };
            'column_order' = @('Date','Breakdown','Ad Type, Product, Site, Ad Unit, Browser Family','Measure','Metric','Metric Average','Metric Z-Score','Revenue Impact')
        },
        @{
            'worksheet' = 'AllMetrics';
            'filetype' = 'csv';
            'attached' = $true;
            'embedded' = $false;
            'filename' = '#{$yesterday}-metric-exception-report';
            'parameters' = @{
                 'Measure' = 'Ad Calls';
                 'Breakdown' = 'Site and Browser Family';
                 'Standard Deviations' = '2';
                 'Drop MinMax' = 'true'
            };
            'column_order' = @('Date','Breakdown','Ad Type, Product, Site, Ad Unit, Browser Family','Measure','Metric','Metric Average','Metric Z-Score','Revenue Impact')
        },
        @{
            'worksheet' = 'AllMetrics';
            'filetype' = 'csv';
            'attached' = $true;
            'embedded' = $false;
            'filename' = '#{$yesterday}-metric-exception-report';
            'parameters' = @{
                 'Measure' = 'Percent Served';
                 'Breakdown' = 'Site and Browser Family';
                 'Standard Deviations' = '2';
                 'Drop MinMax' = 'true'
            };
            'column_order' = @('Date','Breakdown','Ad Type, Product, Site, Ad Unit, Browser Family','Measure','Metric','Metric Average','Metric Z-Score','Revenue Impact')
        },
        @{
            'worksheet' = 'AllMetrics';
            'filetype' = 'csv';
            'attached' = $true;
            'embedded' = $false;
            'filename' = '#{$yesterday}-metric-exception-report';
            'parameters' = @{
                 'Measure' = 'Interaction Rate';
                 'Breakdown' = 'Site and Browser Family';
                 'Standard Deviations' = '2';
                 'Drop MinMax' = 'true'
            };
            'column_order' = @('Date','Breakdown','Ad Type, Product, Site, Ad Unit, Browser Family','Measure','Metric','Metric Average','Metric Z-Score','Revenue Impact')
        },
        @{
            'worksheet' = 'AllMetrics';
            'filetype' = 'csv';
            'attached' = $true;
            'embedded' = $false;
            'filename' = '#{$yesterday}-metric-exception-report';
            'parameters' = @{
                 'Measure' = 'CPI';
                 'Breakdown' = 'Site and Browser Family';
                 'Standard Deviations' = '2';
                 'Drop MinMax' = 'false'
            };
            'column_order' = @('Date','Breakdown','Ad Type, Product, Site, Ad Unit, Browser Family','Measure','Metric','Metric Average','Metric Z-Score','Revenue Impact')
        },
        @{
            'worksheet' = 'AllMetrics';
            'filetype' = 'csv';
            'attached' = $true;
            'embedded' = $false;
            'filename' = '#{$yesterday}-metric-exception-report';
            'parameters' = @{
                 'Measure' = 'Gross Media Revenue';
                 'Breakdown' = 'Site and Browser Family';
                 'Standard Deviations' = '2';
                 'Drop MinMax' = 'true'
            };
            'column_order' = @('Date','Breakdown','Ad Type, Product, Site, Ad Unit, Browser Family','Measure','Metric','Metric Average','Metric Z-Score','Revenue Impact')
        },
        @{
            'worksheet' = 'AllMetrics';
            'filetype' = 'csv';
            'attached' = $true;
            'embedded' = $false;
            'filename' = '#{$yesterday}-metric-exception-report';
            'parameters' = @{
                 'Measure' = 'Ad Calls';
                 'Breakdown' = 'Ad Unit and Browser Family';
                 'Standard Deviations' = '2';
                 'Drop MinMax' = 'true'
            };
            'column_order' = @('Date','Breakdown','Ad Type, Product, Site, Ad Unit, Browser Family','Measure','Metric','Metric Average','Metric Z-Score','Revenue Impact')
        },
        @{
            'worksheet' = 'AllMetrics';
            'filetype' = 'csv';
            'attached' = $true;
            'embedded' = $false;
            'filename' = '#{$yesterday}-metric-exception-report';
            'parameters' = @{
                 'Measure' = 'Percent Served';
                 'Breakdown' = 'Ad Unit and Browser Family';
                 'Standard Deviations' = '2';
                 'Drop MinMax' = 'true'
            };
            'column_order' = @('Date','Breakdown','Ad Type, Product, Site, Ad Unit, Browser Family','Measure','Metric','Metric Average','Metric Z-Score','Revenue Impact')
        },
        @{
            'worksheet' = 'AllMetrics';
            'filetype' = 'csv';
            'attached' = $true;
            'embedded' = $false;
            'filename' = '#{$yesterday}-metric-exception-report';
            'parameters' = @{
                 'Measure' = 'Interaction Rate';
                 'Breakdown' = 'Ad Unit and Browser Family';
                 'Standard Deviations' = '2';
                 'Drop MinMax' = 'true'
            };
            'column_order' = @('Date','Breakdown','Ad Type, Product, Site, Ad Unit, Browser Family','Measure','Metric','Metric Average','Metric Z-Score','Revenue Impact')
        },
        @{
            'worksheet' = 'AllMetrics';
            'filetype' = 'csv';
            'attached' = $true;
            'embedded' = $false;
            'filename' = '#{$yesterday}-metric-exception-report';
            'parameters' = @{
                 'Measure' = 'CPI';
                 'Breakdown' = 'Ad Unit and Browser Family';
                 'Standard Deviations' = '2';
                 'Drop MinMax' = 'false'
            };
            'column_order' = @('Date','Breakdown','Ad Type, Product, Site, Ad Unit, Browser Family','Measure','Metric','Metric Average','Metric Z-Score','Revenue Impact')
        },
        @{
            'worksheet' = 'AllMetrics';
            'filetype' = 'csv';
            'attached' = $true;
            'embedded' = $false;
            'filename' = '#{$yesterday}-metric-exception-report';
            'parameters' = @{
                 'Measure' = 'Gross Media Revenue';
                 'Breakdown' = 'Ad Unit and Browser Family';
                 'Standard Deviations' = '2';
                 'Drop MinMax' = 'true'
            };
            'column_order' = @('Date','Breakdown','Ad Type, Product, Site, Ad Unit, Browser Family','Measure','Metric','Metric Average','Metric Z-Score','Revenue Impact')
        }
    );
    'entities' = @('IM Monitoring')
}