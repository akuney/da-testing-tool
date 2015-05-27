. C:\data\Tableau\Server\Functions\common_functions.ps1

Describe "report_functions: Read-Reports" {

    Context "there is an invalid report" {
        It "returns all the valid reports and no invalid reports" {
            "Not TODO" | Should Be "TODO"
        }
    }

    Context "all reports are valid" {
        It "returns all the reports" {
            "Not TODO" | Should Be "TODO"
        }
    }
}

Describe "report_functions: Read-Report" {
   
    Context "the report is invalid" {
        It "returns an error" {
            "Not TODO" | Should Be "TODO"
        }
    }

    Context "the report is valid" {
        It "returns a valid set of reports" {
            "Not TODO" | Should Be "TODO"
        }
    }
}

Describe "report_functions: Send-Reports" { 
    It "TODO" {
        "Not TODO" | Should Be "TODO"
    }
}

Describe "report_functions: Valid-Report" {
   
    Context "the report is invalid" {
        It "returns an error" {
            "Not TODO" | Should Be "TODO"
        }
    }

    Context "the report is valid" {
        It "returns a valid set of reports" {
            "Not TODO" | Should Be "TODO"
        }
    }
}

Describe "report_functions: Sent-Today" {

    Context "the report has been sent" {
        It "returns true" {
            "Not TODO" | Should Be "TODO"
        }
    }

    Context "the report has not been sent" {
        It "returns false" {
            "Not TODO" | Should Be "TODO"
        }
    }
}

Describe "report_functions: Failed-Today" {

    Context "the report has failed today" {
        It "returns true" {
            "Not TODO" | Should Be "TODO"
        }
    }

    Context "the report has not failed today" {
        It "returns false" {
            "Not TODO" | Should Be "TODO"
        }
    }
}

Describe "report_functions: Scheduled" {
    $report_schedules = @{
	"DailyReport" = "Daily";
	"MondayReport" = "Monday";
	"MonthlyReport" = "Monthly"
    }
    
    Context "Daily Reports" {
	Mock Get-Date {return @{Day=2; DayOfWeek="Tuesday"}}
	
	It "returns daily schedule successfully and non-daily schedules unsuccessfully" {
	    (Scheduled -report "DailyReport") | Should Be $true
	}
	
	It "returns false for weekday not on that weekday" {
	    (Scheduled -report "MondayReport") | Should Be $false
	}
	
	It "returns false for monthly not on the first of the month" {
	    (Scheduled -report "MonthlyReport") | Should Be $false
	}
    
    }
    
    Context "Weekday Reports" {
	Mock Get-Date {return @{DayOfWeek="Monday"}}
	
	It "returns weekday schedule successfully on correct weekday" {
	    (Scheduled -report "MondayReport") | Should Be $true
	}

    }
    
    Context "Monthly Reports" {
	Mock Get-Date {return @{Day=1}}
	
	It "returns monthly schedule successfully on first of the month" {
	    (Scheduled -report "MonthlyReport") | Should Be $true
	}
    
    }	
}

Describe "dependency_functions: Dependencies-Met" {
    $report_dependencies = @{
	"ReportWithOneDependency" = @("WorkbookDependency1");
	"ReportWithTwoDependencies" = @("WorkbookDependency2","WorkbookDependency3")
    }
    
    Context "dependencies not complete with one dependency" {
	
	It "returns false for report with one dependency" {
	    Mock Find-Finish-Message-In-Log {return -1}
	    
	    (Dependencies-Met -report "ReportWithOneDependency") | Should Be $false
	    Assert-MockCalled Find-Finish-Message-In-Log -Exactly 1 {$workbook -eq "WorkbookDependency1"}

	}    
    }
    
    Context "dependencies not complete with two dependencies" {	
	It "returns false for report with multiple dependencies" {
	    Mock Find-Finish-Message-In-Log {return -1}
	    
	    (Dependencies-Met -report "ReportWithTwoDependencies") | Should Be $false
	    Assert-MockCalled Find-Finish-Message-In-Log -Exactly 1 {$workbook -eq "WorkbookDependency2"}
	}    
    }

    Context "dependencies complete with one dependency" {
	
	It "returns true for report with one dependency" {
	    Mock Find-Finish-Message-In-Log {return 1}
	    
	    (Dependencies-Met -report "ReportWithOneDependency") | Should Be $true
	    Assert-MockCalled Find-Finish-Message-In-Log -Exactly 1 {$workbook -eq "WorkbookDependency1"}	
	}
    }

    Context "dependencies complete with two dependencies" {

	It "returns true for report with multiple dependencies" {
	    Mock Find-Finish-Message-In-Log {return 1}
	    
	    (Dependencies-Met -report "ReportWithTwoDependencies") | Should Be $true
	    Assert-MockCalled Find-Finish-Message-In-Log -Exactly 2
	}
    }
}

Describe "report_functions: Build-Report" { 
    It "TODO" {
        "Not TODO" | Should Be "TODO"
    }
}

Describe "report_functions: Get-Asset-Array" { 
    It "TODO" {
        "Not TODO" | Should Be "TODO"
    }
}

Describe "report_functions: Generate-Email-String" { 

    Context "the type is attached" {
        It "returns a comma delimited string of all attached files" {
            "Not TODO" | Should Be "TODO"
        }
    }

    Context "type is embedded" {
        It "returns a comma delimited string of all embedded files" {
            "Not TODO" | Should Be "TODO"
        }
    }
}