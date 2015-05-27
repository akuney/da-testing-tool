# error email properties
$global:error_email_recipient = "reporting@intentmedia.com"
$global:error_email_sender = "reports@intentmedia.com"

$global:max_logging_tries = 10

# FUNCTIONS
#   Block-Until-File-Unlocked -filepath
#   Get-Log-File -name -folder
#   Log-Message -filepath -message
#   Handle-Eror -message -name -folder
#   Send-Error-Email -attachments -subject -message
#   Is-Line-After-Date -line -timestamp
#   Line-Contains-Substrings -line -substrings
#   Find-Line-After-Date -contents -timestamp -substrings


Function Block-Until-File-Unlocked
{
    Param(
        [Parameter(Mandatory=$true)]
        [string]
        $filepath
    )
    Process {
    }
}

Function Get-Log-File
{
    Param(
        [Parameter(Mandatory=$true)]
        [string]
        $name
    ,
        [Parameter(Mandatory=$true)]
        [string]
        $folder
    )
    Process {
        $stripped_name = [System.Text.RegularExpressions.Regex]::Replace($name,"[^1-9a-zA-Z_]"," ")
        $log_file = ([string]::Concat($stripped_name.Replace(" ",""),".log"))
        return "$root_folder\Logs\$folder\$log_file"
    }
}

Function Log-Message
{
    Param(
        [Parameter(Mandatory=$true)]
        [string]
        $filepath
    ,
        [Parameter(Mandatory=$true)]
        [string]
        $message
    )
    Process {
        $current_time = (Get-Current-Time)

        # Log the message with the current time
        "$current_time : $process_id : $message" | Out-File $filepath -append -encoding "UTF8"
    }    
}


Function Handle-Error
{
    Param(
        [Parameter(Mandatory=$true)]
        [string]
        $message
    ,
        [Parameter(Mandatory=$true)]
        [string]
        $name
    ,
        [Parameter(Mandatory=$true)]
        [string]
        $folder
    )
    Process {
        $log_filepath = (Get-Log-File -name $name -folder $folder)
        Log-Message -filepath $log_filepath -message "Handle-Error: $name - ERROR - $message"
        Send-Error-Email -attachments $log_filepath -subject "Problem with $name ($folder)" -message $message        
    }
}

Function Send-Error-Email
{
    Param(
        [Parameter(Mandatory=$true)]
        [string]
        $attachments
    ,
        [Parameter(Mandatory=$true)]
        [string]
        $subject
    ,
        [Parameter(Mandatory=$true)]
        [string]
        $message
    )
    Process {
        & $blat -to $error_email_recipient -body "$process_id : $message" -f $error_email_sender -attach $attachments -s $subject -server $emailserver -u $emailuser -pw $emailpassword -debug | Out-Null
    }
}

Function Is-Line-After-Date
{
    Param(
        [Parameter(Mandatory=$true)]
        [string]
        $line
    ,
        [Parameter(Mandatory=$true)]
        [datetime]
        $timestamp
    )
    Process {
        $line_date = [datetime]($line.Substring(0,19))
        # if the line's date is today, then continue
        return ($line_date -gt $timestamp)
    }
}

Function Line-Contains-Substrings
{
    Param(
        [Parameter(Mandatory=$true)]
        [string]
        $line
    ,
        [Parameter(Mandatory=$true)]
        [string[]]
        $substrings
    )
    Process {
        $i = 0
        while ($i -lt $substrings.Length) {
            if (!($line.Contains($substrings[$i]))) {
                return 0
            }
            $i++
        }
        return 1
    }
}

Function Find-Line-After-Date
{
    Param(
        [string[]]
        $contents
    ,
        [Parameter(Mandatory=$true)]
        [datetime]
        $timestamp
    ,
        [Parameter(Mandatory=$true)]
        [string[]]
        $substrings
    )
    Process {
        $i = 0
        do {
            Try {
                $current_line = ([String]$contents[$i])
                if ($current_line) {
                    # if the line exists, then continue
                    if ((Is-Line-After-Date -line $current_line -timestamp $timestamp) -and (Line-Contains-Substrings -line $current_line -substrings $substrings)) {
                        return $i
                    }
                }
            } Catch {
                # TOOD should something be done here?
            }
            $i++
        } until ($i -eq $contents.Length)
        return -1
    }
}