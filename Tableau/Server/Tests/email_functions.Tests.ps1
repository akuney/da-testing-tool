. C:\data\Tableau\Server\Functions\common_functions.ps1

Describe "email_functions: Email-Report" {
    It "REALLY NEED TO SPLIT THIS OUT" {
        "Not TODO" | Should Be "TODO"
    }
}

Describe "email_functions: Get-Template" {
    It "returns correct template path" {
        (Get-Template($sample_email) -eq "$root_folder\Reports\$sample_email\email_template.html") | Should Be $true
    }
}

Describe "email_functions: Evaluate-Template" {

    It "returns evaluated email template" {
        $sample_report = $reports['sample']
        $sample_report_name = $sample_report.name
        $sample_entity = $sample_report.entities[0]

        Mock Get-Template {}
        Mock Eval-Expressions {}

        (Evaluate-Template -report_name $sample_report_name -entity $sample_entity) | Should Be (Artifacts-Folder -report_name $report_name -entity $entity -archived $false)
    }
}

Describe "email_functions: Format-Embedded-Image" {

    It "returns embedded image string" {
        (Format-Embedded-Image "testing_image") | Should Be "<p><img src=`"cid:testing_image`"><br></p>"
    }
}