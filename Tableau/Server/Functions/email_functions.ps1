# default email parameters
$global:blat="C:\Program Files (x86)\blat\full\blat.exe"
$global:emailserver="mail.authsmtp.com"
$global:emailuser="ac47396"
$global:emailpassword="lOouqtVDx4RJ6PY9IltI"
$global:default_report_sender = "reports@intentmedia.com"
$global:default_bcc = "sarah.jabon@intentmedia.com"

# FUNCTIONS
#   Email-Report -report_name -sender -recipient -cc -bcc -subject -attach -embed
#   Get-Template -report_name
#   Evaluate-Template -report_name -entity
#   Format-Embedded-Image -filepath


Function Email-Report {
    Param(
        [Parameter(Mandatory=$true)]
        [string]
        $report_name
    ,        
        [Parameter(Mandatory=$true)]
        [string]
        $entity
    ,        
        [Parameter(Mandatory=$false)]
        [string]
        $sender = $default_report_sender
    ,
        [Parameter(Mandatory=$true)]
        [string]
        $recipient
    ,
        [Parameter(Mandatory=$false)]
        [string]
        $cc = ""
    ,
        [Parameter(Mandatory=$false)]
        [string]
        $bcc = ""
    ,
        [Parameter(Mandatory=$true)]
        [string]
        $subject
    ,
        [Parameter(Mandatory=$false)]
        [string]
        $attach = ""
    ,
        [Parameter(Mandatory=$false)]
        [string]
        $embed = ""
    ,
        [Parameter(Mandatory=$false)]
        [string]
        $static = ""
    )
    Process {
        Try {
            # set sender
            if (!$sender) {
                $sender = $default_report_sender
            }
            # set attach
            if ($attach) {
                $attached_files = $attach.Split(",")
                foreach ($file in $attached_files) {
                    if (!(Valid-File($file))) {
                        Handle-Error -message "Email-Report: $report_name - Attachment $file is invalid" -name $report_name -folder "Reports"
                        return 0
                    }            
                }
            }
            # set embed
            if ($embed) {
                $embedded_images = ""
                $embedded_files = $embed.Split(",")
                foreach ($file in $embedded_files) {
                    if (!(Valid-File($file))) {
                        Handle-Error -message "Email-Report: $report_name - Embedded $file is invalid" -name $report_name -folder "Reports"
                        return 0
                    } else {
                      $embedded_images += Format-Embedded-Image -filepath $file
                    }            
                }
            }

            if ($static) {
                $embed += ',' + $root_folder + "\ReportImages\" + $static
            }

            if (!(Valid-File(Get-Template($report_name)))) {
                Handle-Error -message "Email-Report: $report_name - Could not find $report_name template" -name $report_name -folder "Reports"
                return 0
            }

            $email_path = (Evaluate-Template -report_name $report_name -entity $entity)

            # create full command and run
            $command = "& `"$blat`" `"$email_path`" -html -to `"$recipient`" -f `"$sender`" -s `"$subject`" -server $emailserver -u $emailuser -pw $emailpassword -debug -log `""
            $command += (Get-Log-File -name $report_name -folder "Reports")
            $command += "`" -timestamp "

            # set bcc
            if ($bcc) {
                $bcc += ",$default_bcc"
                $command += "-bcc `"$bcc`""
            } else {
                if (!$debug) {
                    $command += "-bcc `"$default_bcc`""
                }
            }

            if ($cc) {
                $command += " -cc `"$cc`""
            }
            if ($attach) {
                $command += " -attach `"$attach`""
            }
            if ($embed) {
                $command += " -embed `"$embed`""
            }

            Invoke-Expression -command $command
            if (!$?) {
                Handle-Error -message "Email-Report: $report_name - Invoke-Expression failed with error: $error, command was $command" -name $report_name -folder "Reports"
                return 0
            }

            Log-Message -filepath (Get-Log-File -name $report_name -folder "Reports") -message "Email-Report: $report_name - Sent successfully"
            return 1
        } Catch {
            Handle-Error -message "Email-Report: $report_name - Failed with error: $error" -name $report_name -folder "Reports"
            return 0
        }
    }
}

Function Get-Template
{
    Param(
        [Parameter(Mandatory=$true)]
        [string]
        $report_name
    )
    Process {    
        return "$root_folder\ReportTemplates\$report_name.html"
    }
}

Function Evaluate-Template
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
        $email_template = (Get-Content(Get-Template($report_name)))
        $email_path = (Artifacts-Folder -report_name $report_name -entity $entity -archived $false) + "\email_template.html"
        Eval-Expressions($email_template) | Out-File $email_path -force
        return $email_path
    }
}

Function Format-Embedded-Image
{
   Param(
        [Parameter(Mandatory=$true)]
        [string]
        $filepath
    )
    Process {
        $split_filepath = $filepath.split("\")
        $filename = $split_filepath[$split_filepath.length - 1]
        return "<p><img src=`"cid:" + $filename + "`"><br></p>"   
    }
}