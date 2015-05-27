# date variables
$global:today = (Get-Date).ToString('yyyy-MM-dd')
$global:formatted_date = ((Get-Date).AddDays(-1)).ToString('MMM dd, yyyy')
$global:yesterday = ((Get-Date).AddDays(-1)).ToString('yyyy-MM-dd')
$global:beginning_of_day = (Get-Date).ToString('yyyy-MM-dd 00:00:00')

# process id
$global:process_id = [System.Diagnostics.Process]::GetCurrentProcess().Id

# root folder
$global:root_folder = "C:\data\Tableau\server"
$global:repo_folder = "C:\data"

# debug
$debug = 1

# pdftk
$global:pdftk = "C:\Program Files (x86)\PDF Labs\PDFtk Server\bin\pdftk.exe"

# import necessary files
. $root_folder\Functions\email_functions.ps1
. $root_folder\Functions\logging_and_error_functions.ps1
. $root_folder\Functions\report_functions.ps1
. $root_folder\Functions\tableau_functions.ps1
. $root_folder\References\entities_to_recipients.ps1
. $root_folder\References\workbook_to_site_mapping.ps1

# read reports
$global:report_folder = "$root_folder\ReportDefinitions"
$global:reports = Read-Reports


# FUNCTIONS
#   Create-Folder-If-Needed -filepath
#   Valid-File -path
#   File-Exists -path
#   Get-Files-Matching-Pattern -pattern
#   Eval-Expressions -content
#   Eval-Expression -content
#   Artifacts-Folder -report_name -entity -archived
#   Archive-Artifacts-Folder -report_name -entity
#   Get-Parent-Folder -path
#   Get-Current-Time
#   Sleep-Random -interval
#   Reorder-CSV-Columns -filename -column_order
#   Sort-CSV-by-Date -filename
#   Concat-Files -original -to_add
#   Remove-Spaces -unstripped_string


Function Create-Folder-If-Needed
{
    Param(
        [Parameter(Mandatory=$true)]
        [string]
        $filepath
    )
    Process {
        if (!(Test-Path -Path $filepath)) {
            new-item -Path $filepath -itemtype directory | Out-Null
        }
    }
}

Function Valid-File
{
    Param(
        [Parameter(Mandatory=$true)]
        [string]
        $path
    )
    Process {
        return ((File-Exists($path)) -and ((Get-Content $path).length -gt 1))
    }
}

Function File-Exists
{
    Param(
        [Parameter(Mandatory=$true)]
        [string]
        $path
    )
    Process {
        return ((Get-Files-Matching-Pattern($path)).length -gt 0)
    }
}

Function Get-Files-Matching-Pattern
{
    Param(
        [Parameter(Mandatory=$true)]
        [string]
        $pattern
    )
    Process {
        return (Get-Item $pattern -ea "SilentlyContinue")
    }
}

Function Eval-Expressions
{
    Param(
        [Parameter(Mandatory=$false)]
        [string[]]
        $content
    )
    Process {        
        $evaluated = @()
        for ($i=0; $i -lt $content.length; $i++) {
            if($content[$i]) {
                $evaluated += Eval-Expression($content[$i])
            }
        }        
        return $evaluated
    }
}

Function Eval-Expression
{
    Param(
        [Parameter(Mandatory=$true)]
        [string]
        $content
    )
    Process {    
        $evaluated_content = $content
        $matched_expressions = (Select-String "#{(.*?)}" -input $content -AllMatches | Foreach {$_.matches})
        # matched_expressions is an object if there's one result and an array if not
        # TODO clean this up
        if ($matched_expressions.Groups) {
            for ($i = 0; $i -lt $matched_expressions.Groups.count; $i++) {
                $expression = $matched_expressions.Groups[$i].value 
                $evaluated = Invoke-Expression($expression)
                $evaluated_content = $evaluated_content.Replace("#{$expression}", $evaluated)
            }
        } else {
            for ($i = 0; $i -lt $matched_expressions.length; $i++) {
                $expression = $matched_expressions[$i].Groups[1].value 
                $evaluated = Invoke-Expression($expression)
                $evaluated_content = $evaluated_content.Replace("#{$expression}", $evaluated)
            }        
        }
        return $evaluated_content
    }
}

Function Artifacts-Folder
{
    Param(
        [Parameter(Mandatory=$true)]
        [string]
        $report_name
    ,
        [Parameter(Mandatory=$false)]
        [string]
        $entity
    ,
        [Parameter(Mandatory=$true)]
        [boolean]
        $archived
    )
    Process {
        $path = "$repo_folder"
        $stripped_report = ([System.Text.RegularExpressions.Regex]::Replace($report_name,"[^1-9a-zA-Z_]","")).Replace(" ","")
        $stripped_entity = ([System.Text.RegularExpressions.Regex]::Replace($entity,"[^1-9a-zA-Z_]","")).Replace(" ","")

        if ($archived) {
            $path += "\Archived_Artifacts\$today"
        } else {
            $path += "\Artifacts"
        }

        $path += "\$stripped_report\$process_id\"
        
        if ($entity -and (!($archived))) {
            $path += "$stripped_entity"
        }
        
        Create-Folder-If-Needed -filepath $path
        return $path
    }
}

Function Archive-Artifacts-Folder
{
    Param(
        [Parameter(Mandatory=$true)]
        [string]
        $report_name
    ,
        [Parameter(Mandatory=$false)]
        [string]
        $entity
    )
    Process {
        $archived_artifacts_parent_folder = (Get-Parent-Folder(Artifacts-Folder -report_name $report_name -entity $entity -archived $true))
        $artifacts_folder = (Artifacts-Folder -report_name $report_name -entity $entity -archived $false)
        Create-Folder-If-Needed -filepath $archived_artifacts_parent_folder
        Log-Message -filepath (Get-Log-File -name $report_name -folder "Reports") -message "Archive-Artifacts-Folder: Trying to move $artifacts_folder to $archived_artifacts_parent_folder"
        Move-Item -Path $artifacts_folder -Destination ($archived_artifacts_parent_folder) -force
    }
}

Function Get-Parent-Folder
{
    Param(
        [Parameter(Mandatory=$true)]
        [string]
        $path
    )
    Process {
        return ($path.Substring(0, $path.lastIndexOf("\")))
    }
}

Function Get-Current-Time
{
    Process {
        return (Get-Date).ToString('yyyy-MM-dd HH:mm:ss')
    }
}

Function Sleep-Random
{
    Param(
        [Parameter(Mandatory=$true)]
        [int]
        $inverval
    )
    Process {
        $rand = New-Object system.random
        Start-Sleep -s ($rand.next(0, $interval))
    }
}

Function Reorder-CSV-Columns
{
    Param(
        [Parameter(Mandatory=$true)]
        [string]
        $filename
    ,
        [Parameter(Mandatory=$true)]
        [array]
        $column_order
    )
    Process {
        # TODO add try catch
        $csv_in = Import-CSV $filename
        $csv_in | Select-Object $column_order | Export-CSV $filename -NoTypeInformation
    }
}

Function Sort-CSV-by-Date
{
    Param(
        [Parameter(Mandatory=$true)]
        [string]
        $filename
    )
    Process {
        # TODO add try catch
        $csv_in = Import-CSV $filename
        $csv_in | Sort-Object {$_."Date" -as [datetime]} -descending | Export-CSV $filename -NoTypeInformation
    }
}

Function Concat-Files
{
    Param(
        [Parameter(Mandatory=$true)]
        [string]
        $original
    ,
        [Parameter(Mandatory=$true)]
        [string]
        $to_add
    )
    Process {
        $original_filetype = [IO.Path]::GetExtension($original)
        $to_add_filetype =  [IO.Path]::GetExtension($to_add)

        # TODO unhack these errors
        # error out if filetypes do not match
        if ($original_filetype -ne $to_add_filetype) {    
            Handle-Error -message "Concat-Files: Cannot concatenate files of different types. The types were $original_filetype and $to_add_filetype." -name "general" -folder "Reports"
        # concatenate CSVs
        } elseif ($original_filetype.CompareTo(".csv") -eq 0) {
            $temporary_file = dir $original,$to_add | Import-CSV
            $temporary_file | Export-CSV $original -NoTypeInformation
            $temporary_file = ''
        # concatenate PDFs
        } elseif ($original_filetype.CompareTo(".pdf") -eq 0) {
            $temporary_file = (Get-Parent-Folder($original)) + "\temp.pdf"
            & $pdftk $original $to_add cat output $temporary_file
            Move-Item $temporary_file $original -force
        # error out for unsupported filetypes
        } else {
            Handle-Error -message "Concat-Files: The following files cannot be concatenated: $original and $to_add." -name "general" -folder "Reports"
        }
    }
}

Function Remove-Spaces
{
    Param(
        [Parameter(Mandatory=$true)]
        [string]
        $unstripped_string
    )
    Process {
        return $unstripped_string.Replace(" ","")
    }
}