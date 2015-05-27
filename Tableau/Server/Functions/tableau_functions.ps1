# default Tableau variables
$global:tabcmd="C:\Program Files\Tableau\Tableau Server\8.2\bin\tabcmd.exe"
$global:tabserver="http://localhost"
$global:tabuser_default="reports"
$global:tabuser_preqa="reports_PreQA"
$global:tabuser_underlyingdata="reports_UnderlyingData"
$global:tabpassword="CnEPzdaJUsDGTV6ef3Nk3b3hri"

# log pull frequency
$global:max_number_of_log_pulls = 720
$global:sec_pause_between_log_pulls = 10

# refresh tries
$global:sec_pause_between_refreshes = 5
$global:max_refresh_tries = 5

# FUNCTIONS
#   Get-Tabuser -site
#   Schedule-Extract-Refresh -workbook
#   Update-Tableau-Workbook -workbook -timestamp
#   Get-Log-File-Names
#   Get-Tableau-Log
#   Find-Finish-Message-In-Log -workbook -timestamp
#   Check-Tableau-Log -workbook -timestamp
#   Get-Asset-From-Tableau -asset
#   Get-Parameter-String -parameters

Function Get-Tabuser
{
    Param(
        [Parameter(Mandatory=$true)]
        [string]
        $site
    )
    Process {
        switch ($site)
        {
            "`"`"" {$tabuser=$tabuser_default}
            "DataSandbox" {$tabuser=$tabuser_default}
            "PreQA" {$tabuser=$tabuser_preqa}
            "UnderlyingData" {$tabuser=$tabuser_underlyingdata}
        }
        return $tabuser
    }
}

Function Schedule-Extract-Refresh
{
    Param(
        [Parameter(Mandatory=$true)]
        [string]
        $workbook
    )
    Process {
        # tabcmd takes workbook name in without spaces
        $workbook_stripped = $workbook.Replace(" ","")
        $site = ($workbook_to_site[$workbook])        
        $tabuser = Get-Tabuser($site)

        $i = 0
        do {
            $i++    
            Log-Message -filepath (Get-Log-File -name $workbook -folder "Workbooks") -message "Schedule-Extract-Refresh: $workbook - Try number $i to schedule extract refresh on the site $site."

            # Schedule update
            & $tabcmd refreshextracts --url $workbook_stripped -s $tabserver -t $site -u $tabuser -p $tabpassword | Out-File (Get-Log-File -name $workbook -folder "Workbooks") -append -encoding "UTF8"
            
        } until ($? -or ($i -ge $max_refresh_tries))
        
        if ($?) {
            Log-Message -filepath (Get-Log-File -name $workbook -folder "Workbooks") -message "Schedule-Extract-Refresh: $workbook - scheduled to refresh on the site $site now."
            return 1
        } else {
            Handle-Error -message "Schedule-Extract-Refresh: $workbook - Not successfully refreshed on site $site. The tabcmd refreshextracts command failed" -name $workbook -folder "Workbooks"
            return 0
        }
    } 
} 

Function Update-Tableau-Workbook
{
    Param(
        [Parameter(Mandatory=$true)]
        [string]
        $workbook
    ,
        [Parameter(Mandatory=$true)]
        [datetime]
        $timestamp
    )
    Process {
        Try  {
            # schedule refresh
            if (Schedule-Extract-Refresh -workbook $workbook){
                # check for update in logs
                if (Check-Tableau-Log -workbook $workbook -timestamp $timestamp) {
                    Log-Message -filepath (Get-Log-File -name $workbook -folder "Workbooks") -message "Update-Tableau-Workbook: $workbook - extract refresh finished successfully"    
                    return 1
                } else {
                    Handle-Error -message "Update-Tableau-Workbook: $workbook - Could not find update in Tableau logs after $date_time_of_call" -name $workbook -folder "Workbooks"
                    return 0
                }
            }
        } Catch {
            Handle-Error -message "Update-Tableau-Workbook: $workbook - failed with: $error" -name $workbook -folder "Workbooks"
            return 0
        }
    }
}

Function Get-Log-File-Names
{
    Process {
        # combine the last two log files (because Tableau stores 24 hours in each, but not starting at midnight)
        $today_log_file = "C:\ProgramData\Tableau\Tableau Server\data\tabsvc\logs\backgrounder\backgrounder-*.log"

        $log_files = @($today_log_file)
        $existing_log_files = @()
        foreach($log_file_pattern in $log_files) {
            foreach($log_file in (Get-Files-Matching-Pattern($log_file_pattern))) {
                $existing_log_files += $log_file.FullName
            } 
        }
        return $existing_log_files
    }
}

Function Get-Tableau-Log
{
    Process {
        $tableau_log = @()
        foreach ($log in (Get-Log-File-Names))
        {
            Sleep-Random(10)
            $tableau_log += Get-Content($log)
        }
        return $tableau_log
    }    
}

Function Find-Finish-Message-In-Log
{
    Param(
        [Parameter(Mandatory=$true)]
        [string]
        $workbook
    ,
        [Parameter(Mandatory=$true)]
        [datetime]
        $timestamp
    )
    Process {
        $workbook_stripped = $workbook.Replace(" ","")
        return (Find-Line-After-Date -contents (Get-Tableau-Log) -timestamp $timestamp -substrings @("Storing to repository: $workbook_stripped/extract"))
    }
}

Function Check-Tableau-Log 
{
    Param(
        [Parameter(Mandatory=$true)]
        [string]
        $workbook
    ,
        [Parameter(Mandatory=$true)]
        [datetime]
        $timestamp
    )
    Process {
        # initialize variables
        $times_log_was_pulled = 0
        
        # this loops $max_number_of_log_pulls times. It pulls Tableau logs and checks for the refresh. This loop will only finish if the refresh is not found.
        do {

            $line_number = Find-Finish-Message-In-Log -workbook $workbook -timestamp $timestamp

            if ($line_number -lt 0) {        
                # No refresh was found yet, so sleeping for $seconds_to_wait and then trying again
                Log-Message -filepath (Get-Log-File -name $workbook -folder "Workbooks") -message "Check-Tableau-Log: $workbook - Try number $times_log_was_pulled. Sleeping for $sec_pause_between_log_pulls before trying to hit Tableau again."
                Start-Sleep -s $sec_pause_between_log_pulls                        
            }    
            
            $times_log_was_pulled++
            
        } until (($times_log_was_pulled -ge $max_number_of_log_pulls) -or ($line_number -ge 0))

        if ($line_number -ge 0) {
            Log-Message -filepath (Get-Log-File -name $workbook -folder "Workbooks") -message "Check-Tableau-Log: $workbook - Found a refresh message in the log file at line $line_number (zero based)."                                    
            return 1
        } else {
            Handle-Error -message "Check-Tableau-Log: $workbook - An update was not found after repeating $times_log_was_pulled times" -name $workbook -folder "Workbooks"
            return 0
        }
    } 
}

Function Get-Asset-From-Tableau
{
    Param(
        [Parameter(Mandatory=$true)]
        [object]
        $asset
    )
    Process {
        $formatted_parameters = ""
        if ($asset.PSObject.Properties.Match("parameters").Count -ne 0) {
            $formatted_parameters = Get-Parameter-String -parameters $asset.parameters
        }
        $url = $asset.workbook + "/" + $asset.worksheet + $formatted_parameters
        $filetype_param = "--" + $asset.filetype

        $page_layout = ''
        if ($asset.page_layout) {
            $page_layout = '--pagelayout ' + $asset.page_layout
        }

        $folder = Artifacts-Folder -report_name $asset.report_name -entity $asset.entity -archived $false
        $full_filename = $folder + "\" + $asset.filename + "." + $asset.filetype
       
        $tabsite = $asset.site
        $tabuser = Get-Tabuser($tabsite)

        $site_param = ''
        if ($tabsite -ne "`"`"") {
            $site_param = "-t `"$tabsite`" "
        }

        # TODO clean this up
        # TODO add try catch
        if (Test-Path -Path $full_filename) {
            $full_filename_temp =  $folder + "\" + $asset.filename + "_temp" + "." + $asset.filetype
            $tabcmd_command = "& `"$tabcmd`" export -s `"http://localhost`" --no-cookie $site_param -u `"$tabuser`" -p `"$tabpassword`" `"$url`" $filetype_param $page_layout --timeout 900 -f `"$full_filename_temp`""
            Invoke-Expression $tabcmd_command
            Concat-Files -original $full_filename -to_add $full_filename_temp

            # TODO this file really should exist. if it doesn't this should break and send an error email
            If (Test-Path $full_filename_temp) {
                Remove-Item $full_filename_temp
            }
        } else {
            $tabcmd_command = "& `"$tabcmd`" export -s `"http://localhost`" --no-cookie $site_param -u `"$tabuser`" -p `"$tabpassword`" `"$url`" $filetype_param $page_layout --timeout 900 -f `"$full_filename`""
            Invoke-Expression $tabcmd_command
        }

        if ($asset.column_order) {
            Reorder-CSV-Columns -filename $full_filename -column_order $asset.column_order
            Sort-CSV-by-Date -filename $full_filename
        }
    }
}

Function Get-Parameter-String
{
    Param(
        [Parameter(Mandatory=$true)]
        [hashtable]
        $parameters
    )
    Process {
        # format into URL and replace spaces with plusses
        if ($parameters.count -gt 0) {
            $urlized_parameters = "?"
            foreach ($param in $parameters.Keys) {
                $urlized_parameters += $param.Replace(" ","+") + "=" + ($parameters[$param]).Replace(" ","+") + "&"
            }
            # remove trailing ampersand
            return $urlized_parameters.Substring(0,$urlized_parameters.length - 1)
        } else {
            return ""
        }   
    }
}