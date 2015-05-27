. C:\data\Tableau\Server\Functions\common_functions.ps1
$reports = Read-Reports("C:\data\Tableau\Server\Tests\Fixtures\ReportDefinitions")

Describe "common_functions: Create-Folder-If-Needed" {

    Context "folder does not exist" {
        Mock New-Item {}
        It "creates new folder if needed" {
            Create-Folder-If-Needed -filepath "TestDrive:\NewDrive"
            Assert-MockCalled New-Item -Exactly 1 {$Path -eq "TestDrive:\NewDrive"}
        }
    }

    Context "folder does exist" {
        Mock New-Item {}
        It "does nothing if folder exists" {
            Create-Folder-If-Needed("TestDrive:\")
            Assert-MockCalled New-Item -Exactly 0
	}
    }
}

Describe "common_functions: Valid-File" {

    It "fails if invalid file" {
        Valid-File("TestDrive:\a_file_that_does_not_exist.ps1") | Should Be $false
    }

    It "fails if empty file" {
        " " > "TestDrive:\empty_file.txt"
        Valid-File("TestDrive:\empty_file.txt") | Should Be $false
    }

    It "succeeds if valid file" {
        "a file that exists\r\nis this file right here\r\n\it has three lines" > "TestDrive:\existing_file.txt"
        Valid-File("TestDrive:\existing_file.txt") | Should Be $true
    }
}

Describe "common_functions: File-Exists" {

    It "fails if invalid file" {
        File-Exists("C:\data\Tableau\Server\Functions\a_file_that_does_not_exist.ps1") | Should Be $false
    }
    
    It "succeeds if valid file" {
        File-Exists("C:\data\Tableau\Server\Functions\common_functions.ps1") | Should Be $true
    }

}

Describe "common_functions: Get-Files-Matching-Pattern" {

    It "finds correct number of files" {
	New-Item -Path "TestDrive:\File1" -Type "File" | Out-Null
	New-Item -Path "TestDrive:\File2" -Type "File" | Out-Null
	(Get-Files-Matching-Pattern("TestDrive:\File*")).Length | Should Be 2
    }
}

Describe "common_functions: Eval-Expressions" {

    It "evaluates single expressions" {
        $to_evaluate_1 = "answer1"
        $to_evalueate_2 = "answer2"
        Eval-Expressions(@($to_evaluate_1, $to_evaluate_2)) | Should Be @("answer1","answer2")
    }
    
    It "evaluates multiple expressions" {
        $to_evaluate_1 = "answer1"
        $to_evaluate_2 = "answer2"
        Eval-Expressions(@("$to_evaluate_1 and $to_evaluate_2")) | Should Be @("answer1 and answer2")
    }
    
    It "returns unchanged expression if nothing to evaluate" {
        Eval-Expressions(@("unchanged1","unchanged2")) | Should Be @("unchanged1","unchanged2")
    }
}

Describe "common_functions: Eval-Expression" {

    It "evaluates single expressions" {
        $to_evaluate = "answer"
        Eval-Expression($to_evaluate) | Should Be "answer"
    }
    
    It "evaluates multiple expressions" {
        $to_evaluate_1 = "answer1"
        $to_evaluate_2 = "answer2"
        Eval-Expressions("$to_evaluate_1 and $to_evaluate_2") | Should Be "answer1 and answer2"
    }
    
    It "returns unchanged expression if nothing to evaluate" {
        Eval-Expressions("unchanged") | Should Be "unchanged"
    }
}

Describe "common_functions: Artifacts-Folder" {

    Context "sample report" {        
        $sample_report = $reports['sample']
        $sample_report_name = $sample_report.name
        $sample_entity = $sample_report.entities[0]

        It "returns path when entity is not null and archived is true" {
            (Artifacts-Folder -report_name $sample_report_name -entity $sample_entity -archived $true) | Should Be "$root_folder\Archived_Artifacts\$today\$sample_report_name\$process_id\$sample_entity"
        }
        
        It "returns path when entity is not null and archived is false" {
            (Artifacts-Folder -report_name $sample_report_name -entity $sample_entity -archived $false) | Should Be "$root_folder\Artifacts\$sample_report_name\$process_id\$sample_entity"
        }
    }
}

Describe "common_functions: Archive-Artifacts-Folder" {

    Context "sample report" {
        Mock Move-Item {}
        $sample_report = $reports['sample']
        $sample_report_name = $sample_report.name
        $sample_entity = $sample_report.entities[0]

        It "archives artifacts folder when entity is not null" {
            Archive-Artifacts-Folder -report_name $sample_report_name -entity $sample_entity
            Assert-MockCalled move-item -Exactly 1 {$Path -eq ""; $Destination -eq ""}
        }
    }
}

Describe "common_functions: Get-Parent-Folder" {

    It "returns parent folder successfully" {
        $path = "TestDrive:\AnyFolder"
        (Get-Parent-Folder -path $path) | Should Be "TestDrive:"
    }
}

Describe "common_functions: Get-Current-Time" {
    # intentionally left blank
}

Describe "common_functions: Sleep-Random" {
    # intentionally left blank
}

Describe "common_functions: Reorder-CVS-Columns" {

    Context "CSV should be sorted" {
        It "sorts the CSV columns" {
            "not TODO" | Should Be "TODO" 
        }
        It "has same number of rows after sorting" {
            "not TODO" | Should Be "TODO"
        }
    }

    Context "CSV should not be sorted" {
        It "leaves the file as is" {
            "not TODO" | Should Be "TODO"
        } 
    }
}

Describe "common_functions: Sort-CSV-by-Date" {

    Context "CSV should be sorted" {
        It "sorts the CSV rows" {
            "not TODO" | Should Be "TODO" 
        }
        It "has same number of rows after sorting" {
            "not TODO" | Should Be "TODO"
        }
    }

    Context "CSV should not be sorted" {
        It "leaves the file as is" {
            "not TODO" | Should Be "TODO"
        } 
    }
}

Describe "common_functions: Concat-Files" {

    Context "filetypes to do not match" {
        It "errors out" {
            "not TODO" | Should Be "TODO" 
        }
    }

    Context "filetypes are unsupported" {
        It "errors out" {
            "not TODO" | Should Be "TODO" 
        }
    }

    Context "CSVs" {
        It "concats successfully" {
            "not TODO" | Should Be "TODO" 
        }
    }

    Context "PDFs" {
        It "concats successfully" {
            "not TODO" | Should Be "TODO" 
        }
    }
}

Describe "common_functions: Remove-Spaces -unstripped_string" {

    It "returns string without spaces" {
        (Remove-Spaces -unstripped_string "remove multiple spaces") | Should Be "removemultiplespaces"
    }
}