. C:\data\Tableau\Server\Functions\common_functions.ps1

Describe "tableau_functions: Get-Tabuser" {
    It "returns tabuser_default if default site" {
        (Get-Tabuser -site "`"`"") | Should Be $tabuser_default
    }
    
    It "returns tabuser_underlyingdata if UnderlyingData site" {
        (Get-Tabuser -site "UnderlyingData") | Should Be $tabuser_underlyingdata
    }
    
    It "returns tabuser_preqa if PreQA site" {
        (Get-Tabuser -site "PreQA") | Should Be $tabuser_preqa
    }
}

Describe "tableau_functions: Schedule-Extract-Refresh" {
    It "TODO" {
        "Not TODO" | Should Be "TODO"
    }
}

Describe "tableau_functions: Update-Tableau-Workbook" {
    It "TODO" {
        "Not TODO" | Should Be "TODO"
    }
}

Describe "tableau_functions: Get-Log-File-Names" {
    It "TODO" {
        "Not TODO" | Should Be "TODO"
    }
}

Describe "tableau_functions: Get-Tableau-Log" {
    It "TODO" {
        "Not TODO" | Should Be "TODO"
    }
}

Describe "tableau_functions: Find-Finish-Message-In-Log" {

    Context "message is there" {
        Mock Get-Tableau-Log { return "2013-07-01 11:58:24 $sample_workbook: Finished refresh of extracts" }
        It "returns true when message is there" {
            Find-Finish-Message-In-Log -workbook $sample_workbook -timestamp '2013-07-01 00:00:00' | Should Be 0
        }
    }

    Context "message is not there" {
        Mock Get-Tableau-Log { return "2013-07-01 11:58:24 $sample_workbook" }
        It "returns true when message is there" {
            Find-Finish-Message-In-Log -workbook $sample_workbook -timestamp '2013-07-01 00:00:00' | Should Be -1
        }
    }
}

Describe "tableau_functions: Check-Tableau-Log" {
    It "TODO" {
        "Not TODO" | Should Be "TODO"
    }
}

Describe "tableau_functions: Get-Asset-From-Tableau" {
    It "TODO" {
        "Not TODO" | Should Be "TODO"
    }
}

Describe "tableau_functions: Get-Parameter-String" {
    It "TODO" {
        "Not TODO" | Should Be "TODO"
    }
}