# FUNCTIONS
#   List-Reports
#   Read-Reports
#   Read-Report -file
#   Send-Reports
#   Send-Report -report_name
#   Valid-Report -report
#   Sent-Today -report
#   Failed-Today -report
#   Scheduled -report
#   Dependencies-Met -report
#   Get-Dependencies -report
#   Build-Report -report
#   Get-Distinct-Filepaths -evaluated_assets
#   Get-Asset-Array -assets -report_name -entity -default_workbook
#   Generate-Email-String -asset_array -entity -type


Function List-Reports
{
    Process {
        echo $reports.keys
    }
}

Function Read-Reports
{
    Process {
        $reports_in = @{}
        $file_array = [IO.Directory]::GetFiles($report_folder)
        foreach ($file in $file_array) {
            $report = Read-Report($file)
            if ($report) {
                $reports_in.add($report.name, $report)
                $report = ""
            }
        }
        return $reports_in
    }
}

Function Read-Report
{
    Param(
        [Parameter(Mandatory=$true)]
        [string]
        $file
    )
    Process {
        . $file
        $report_name = [IO.Path]::GetFileNameWithoutExtension($file)
        $report.add('name', $report_name)
        if (Valid-Report($report)) {
            return $report
        } else {
            Handle-Error -message "The following report definition was invalid: $file" -name "$report_name" -folder "Reports"
            return ""
        }
    }
}

Function Send-Reports
{
    Process {
        Sleep-Random(10)

        foreach ($report_name in $reports.keys) {
            $report = $reports[$report_name]
            if ((Dependencies-Met($report)) -and (Scheduled($report)) -and (!(Sent-Today($report)))  -and (!(Failed-Today($report)))) {
                Log-Message -filepath (Get-Log-File -name $report_name -folder "Reports") -message "Send-Reports: $report_name - Ready to send."
                Build-Report -report $report
            } else {
                Log-Message -filepath (Get-Log-File -name $report_name -folder "Reports") -message "Send-Reports: $report_name - Does not need to be sent at this time"
            }
        }
    }
}

Function Send-Report
{
    Param(
        [Parameter(Mandatory=$true)]
        [string]
        $report_name
    )
    Process {
        $report = $reports[$report_name]
        if ($report) {
            Build-Report -report $report
        } else {
            echo "That report doesn't exist in reports."
        }
    }
}

Function Valid-Report
{
    Param(
        [Parameter(Mandatory=$true)]
        [hashtable]
        $report
    )
    Process {
       if ($report.subject -and $report.frequency -and $report.entities -and $report.assets) {
           $assets = $report.assets
           foreach ($asset in $assets) {
               if (!($asset.worksheet -and $asset.filetype -and $asset.filename)) {
                   return $false   
               }
               # TODO add column_order CSV validation
           }
       } else {
           return $false
       }
       return $true
    }
}

Function Sent-Today
{
    Param(
        [Parameter(Mandatory=$true)]
        [hashtable]
        $report
    )
    Process {
        $report_name = $report.name
        if (Get-Content((Get-Log-File -name $report_name -folder "Reports"))) {    
            $report_log = Get-Content((Get-Log-File -name $report_name -folder "Reports"))        
            $line_report_sent = Find-Line-After-Date -contents $report_log -timestamp $beginning_of_day -substrings @("Build-Report: Beginning to build $report_name")
            $report_sent_today = ($line_report_sent -ge 0)
            Log-Message -filepath (Get-Log-File -name $report.name -folder "Reports") -message "Sent-Today: $report_name - Return $report_sent_today"
            return $report_sent_today
        } else {
            return 0
        }
    }
}

Function Failed-Today
{
    Param(
        [Parameter(Mandatory=$true)]
        [hashtable]
        $report
    )
    Process {
        $report_name = $report.name
        if (Get-Content((Get-Log-File -name $report_name -folder "Reports"))) {    
            $report_log = Get-Content((Get-Log-File -name $report_name -folder "Reports"))        
            $report_failed_today = ((Find-Line-After-Date -contents $report_log -timestamp $beginning_of_day -substrings @("Handle-Error: $report_name - ERROR")) -ge 0)
            Log-Message -filepath (Get-Log-File -name $report_name -folder "Reports") -message "Failed-Today: $report_name - Return $report_failed_today"
            return $report_failed_today
        } else {
            return 0
        }
    }
}

Function Scheduled
{
    Param(
        [Parameter(Mandatory=$true)]
        [hashtable]
        $report
    )
    Process {

        if ($report.frequency) {
            $scheduled = (($report.frequency -eq (Get-Date).DayOfWeek) -or ($report.frequency -eq "Daily") -or (($report.frequency -eq "Monthly") -and ((Get-Date).Day -eq 1)))
        } else {
            Handle-Error -message "Scheduled: $report.name did not have a frequency. " -name $report.name -folder "Reports" 
            $scheduled = 0
        }
        return $scheduled
    }
}

Function Dependencies-Met
{
    Param(
        [Parameter(Mandatory=$true)]
        [hashtable]
        $report
    )
    Process {
        $report_name = $report.name
        Log-Message -filepath (Get-Log-File -name $report_name -folder "Reports") -message "Dependencies-Met: $report_name - Checking dependencies"

        $dependencies = Get-Dependencies($report)
        $dependencies_met = 1
        $i = 0
        while (($dependencies_met -gt 0) -and ($i -lt $dependencies.Length)) {
            $dependencies_met = Find-Finish-Message-In-Log -workbook $dependencies[$i] -timestamp $beginning_of_day
            $i++
        }

        Log-Message -filepath (Get-Log-File -name $report_name -folder "Reports") -message "Dependencies-Met: $report_name - Returned $dependencies_met"
        return ($dependencies_met -ge 0)
    }
}

# TODO add try catch
Function Get-Dependencies
{
    Param(
        [Parameter(Mandatory=$true)]
        [hashtable]
        $report
    )
    Process {
        $dependencies = @($report.default_workbook)
        foreach($asset in $report.assets) {
            if ($asset.workbook) {
                $dependencies += $asset.workbook
            }
        }
        return , $dependencies
    }
}

Function Build-Report
{
    Param(
        [Parameter(Mandatory=$true)]
        [hashtable]
        $report
    )
    Process {
        # TODO remove duplication
        $report_name = $report.name
        Log-Message -filepath (Get-Log-File -name $report_name -folder "Reports") -message "Build-Report: Beginning to build $report_name"

        # get entities and determine type of report
        $entities = $report["entities"]
        if ($entities.GetType().Name -eq 'Hashtable') {
            $entity_list = ($entities.Keys) | Sort
            $has_loop_through_param = $true
        } elseif ($entities.GetType().BaseType.Name -eq 'Array') {
            $entity_list = $entities | Sort
            $has_loop_through_param = $false
        } else {
            Handle-Error -message "General-Build-Report: Email $report_name - The type of of entities is unsupported." -name $report_name -folder "Reports"
        }

        foreach ($entity in $entity_list) {
            # retrieve recipients for entity
            $recipient_to = $entities_to_recipients[$entity]            

            # TODO each of these need to be checked before sending
            $assets = $report["assets"]
            $name = $report["name"]
            $subject = Eval-Expressions($report["subject"])
            $default_workbook = $report["default_workbook"]

            # optional
            $sender = if ($report["sender"]) {$report["sender"]} else {""}
            $recipient_cc = if ($report["recipient_cc"]) {$report["recipient_cc"]} else {""}
            $recipient_bcc = if ($report["recipient_bcc"]) {$report["recipient_bcc"]} else {""}

            # create array of assets to pull from Tableau
            if ($has_loop_through_param) {
                $evaluated_assets = @() 
                foreach ($loop_through_parameter in $entities[$entity]) {
                    $evaluated_assets += Get-Asset-Array -assets $assets -report_name $name -entity $entity -default_workbook $default_workbook
                }
            } else {
                $evaluated_assets =  Get-Asset-Array -assets $assets -report_name $name -entity $entity -default_workbook $default_workbook
            }

            # pull assets from Tableau
            foreach ($asset in $evaluated_assets) {
                Get-Asset-From-Tableau -asset $asset
            }

            # SHITTIEST HACK EVER. DON'T DO THIS.
            if (($report_name).CompareTo('Internal_ExceptionReport') -eq 0) {
                # TODO there needs to be some way of applying sort_by to an individual asset instead of all assets
                $documents = Get-Distinct-Filepaths($evaluated_assets)
                foreach ($asset_filepath in $documents) {
                    # this could probably be one step but fuck it. This should all be replaced soon anyway. Also, reading into variables is because PowerShell is stupid
                    # remove quotes
                    $report_with_quotes = Get-Content $asset_filepath
                    $report_with_quotes | Foreach-Object {$_ -replace "`"", ""} | Set-Content $asset_filepath
                    # sort rows
                    $unsorted_csv = Import-CSV $asset_filepath
                    $unsorted_csv | Sort-Object 'Ad Type', 'Product', 'Site', 'Ad Unit', 'Browser' | Export-CSV $asset_filepath -NoTypeInformation
                }
            }

            # I WAS WRONG. THIS IS THE SHITTIEST HACK EVER.
            if (($report_name).CompareTo('Internal_TotalGrossRevenue') -eq 0) {
                $documents = Get-Distinct-Filepaths($evaluated_assets)
                foreach ($asset_filepath in $documents) {
                    $unformatted_subject = (Get-Content $asset_filepath)[1]
                    $still_unformatted_subject = $unformatted_subject.SubString($unformatted_subject.IndexOf(',') + 1)
                    $subject = ($still_unformatted_subject.SubString($still_unformatted_subject.IndexOf(',') + 1)).Replace("`"","").Replace("`$","```$")
                }
            }

            # generate lists of attached and embedded images
            $attached_files = Generate-Email-String -asset_array $evaluated_assets -entity $entity -type "attached"
            $embedded_files = Generate-Email-String -asset_array $evaluated_assets -entity $entity -type "embedded"


            # MORE SHITTY HACKS. GOOD THING THE AIR TEAM IS GONNA FIX EVERYTHING
            if (($report_name).CompareTo('Internal_AfTModelZeroBetas') -eq 0) {
                # TODO there needs to be some way of applying sort_by to an individual asset instead of all assets
                $documents = Get-Distinct-Filepaths($evaluated_assets)
                foreach ($asset_filepath in $documents) {
                    # check filesize
                    if ((Get-Item $asset_filepath).length -lt 3kb) {
                        # need to get out of sending the email. for now, just set asset_filepath to something not legit so the email will error out
                        $attached_files = 'asdfa';
                    }
                }
            }

            # email report
            Try {
                Email-Report -report_name $name -entity $entity -sender $sender -recipient $recipient_to -cc $recipient_cc -bcc $recipient_bcc -subject $subject -attach $attached_files -embed $embedded_files -static $report.static_image
            } Catch {
                Handle-Error -message "Build-Report: Email $name - Failed with error: $error" -name $name -folder "Reports"
            }

            Archive-Artifacts-Folder -report_name $report_name -entity $entity
        }
    }
}

Function Get-Distinct-Filepaths
{
    Param(
        [Parameter(Mandatory=$true)]
        [array]
        $evaluated_assets
    )
    Process{
        $filepath_array = @()
        foreach ($asset in $evaluated_assets)
        {
            $folder = Artifacts-Folder -report_name $asset.report_name -entity $asset.entity -archived $false
            $to_add = $folder + "\" + $asset.filename + "." + $asset.filetype
            if (-not ($filepath_array -contains $to_add)) {
                $filepath_array += $to_add
            }
        }
        return $filepath_array
    }
}

Function Get-Asset-Array
{
    Param(
        [Parameter(Mandatory=$true)]
        [array]
        $assets
    ,
        [Parameter(Mandatory=$true)]
        [string]
        $report_name
    ,
        [Parameter(Mandatory=$true)]
        [string]
        $entity
    ,
        [Parameter(Mandatory=$true)]
        [string]
        $default_workbook
    )
    Process {
        $all_assets = @()
        foreach ($asset in $assets) {
            # turn hash into properties of object
            $asset_object = New-Object -TypeName PSObject -Prop $asset
            $asset_object | Add-Member -MemberType NoteProperty -Name "name" -Value $asset_name
            $asset_object | Add-Member -MemberType NoteProperty -Name "report_name" -Value $report_name
            $asset_object | Add-Member -MemberType NoteProperty -Name "entity" -Value $entity

            # if no workbook provided, use default_workbook
            if ($asset_object.PSObject.Properties.Match("workbook").Count -eq 0) {
                $asset_object | Add-Member -MemberType NoteProperty -Name "workbook" -Value $default_workbook
            }

            # TODO validate site aka remove spaces etc
            # add site
            $asset_object | Add-Member -MemberType NoteProperty -Name "site" -Value $workbook_to_site[$asset_object.workbook]

            # format workbook and worksheet
            $asset_object.workbook = Remove-Spaces -unstripped_string $asset_object.workbook
            $asset_object.worksheet = Remove-Spaces -unstripped_string $asset_object.worksheet

            # evaluate and strip filename
            $asset_object.filename = Remove-Spaces -unstripped_string (Eval-Expressions($asset_object.filename))

            # evaluate parameters
            if ($asset_object.PSObject.Properties.Match("parameters").Count -ne 0) {
                $evaluated_parameters = $asset_object.parameters.clone()
                foreach ($param_name in $asset_object.parameters.Keys) {
                    $evaluated_parameters[$param_name] = Eval-Expressions($asset_object.parameters[$param_name])
                }
                $asset_object.parameters = $evaluated_parameters
            }

            # add asset to list of assets
            $all_assets += $asset_object
        }
        $sorted_assets = $all_assets | Sort-Object 'page_number'
        return , $sorted_assets
    }
}

Function Generate-Email-String
{
    Param(
        [Parameter(Mandatory=$true)]
        [array]
        $asset_array
    ,
        [Parameter(Mandatory=$true)]
        [string]
        $entity
    ,
       [Parameter(Mandatory=$true)]
        [string]
        $type
    )
    Process {
        # add filenames to list if they aren't already included
        $email_string = ""
        foreach ($asset in $asset_array)
        {
           if ($asset.$type) {
                $folder = Artifacts-Folder -report_name $asset.report_name -entity $entity -archived $false
                $to_add = $folder + "\" + $asset.filename + "." + $asset.filetype + ","
                if (-not $email_string.Contains($to_add)) {
                    $email_string += $to_add
                }
            }
        }
        # remove trailing comma
        if ($email_string[$email_string.length -1] -eq ",") {
            return $email_string.Substring(0,$email_string.length -1)
        } else {
            return $email_string
        }
    }
}