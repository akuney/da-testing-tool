. C:\data\Tableau\Server\Functions\common_functions.ps1

Describe "logging_and_error_functions: Get-Log-File" {

    It "returns log file" {
        Get-Log-File -name "name" -folder "folder" | Should Be "$root_folder\Logs\$folder\$log_file"
    }

}

Describe "logging_and_error_functions: Log-Message" {
    
    It "logs message" {
        Mock Out-File {}

        Log-Message -filepath "TestPath:\" -message "sample message"
        Assert-MockCalled Out-File -1 {$filepath -eq "TestPath:\"; $message -eq "sample message"}
    
    }
}

Describe "logging_and_error_functions: Log-Error-Message" {

    It "logs error message" {
        Mock Out-File {}

        Log-Error-Message -message "sample message"
        Assert-MockCalled Out-File 1 {$message -eq "sample message"}
    }

}

Describe "logging_and_error_functions: Handle-Error" {

    It "logs error message in log, and also sends an email" {
        Mock Log-Message {}
        Mock Send-Error-Email {}

        Handle-Error -message "sample message" -name "sample name" -folder "sample folder name"

        Assert-MockCalled Log-Message -Exactly 1 {$message -eq "Handle-Error: sample name - ERROR - sample message"}
        Assert-MockCalled Send-Error-Email -Exactly 1 {$subject -eq "Problem with sample name (sample folder name)"; $message -eq "sample message"}
    }
}

Describe "logging_and_error_functions: Send-Error-Email" {
    It "sends error email" {
        "Not TODO" | Should Be "TODO"
 
    }
}

Describe "logging_and_error_functions: Is-Line-After-Date" {

    It "returns false when the line is before the date" {
        Is-Line-After-Date -line "2013-07-01 11:58:24 line text" -timestamp "2013-07-02 00:00:00" | Should Be $false
    }
    
    It "returns true when the line is after the date" {
        Is-Line-After-Date -line "2013-07-01 11:58:24 line text" -timestamp "2013-07-01 00:00:00" | Should Be $true
    }
}

Describe "logging_and_error_functions: Line-Contains-Substrings" {

    It "returns true when the line contains substrings" {
        Line-Contains-Substrings -line "first second third" -substrings @("first", "second") | Should Be 1
    }

    It "returns false when the line does not contain a substring" {
        Line-Contains-Substrings -line "first second third" -substrings @("first", "second", "fourth") | Should Be 0
    }
}

Describe "logging_and_error_functions: Find-Line-After-Date" {
    
    Context "line exists" {
        It "finds line after date" {
            "Not TODO" | Should Be "TODO"
        }
    }
}