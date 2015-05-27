param (
    [string]$workbook
)

. C:\data\Tableau\Server\Functions\common_functions.ps1

Function Report-Trigger
{
    Param(
        [Parameter(Mandatory=$true)]
        [string]
        $workbook
    )
    Process {
        Sleep-Random(10)
        Try {
            Log-Message -filepath (Get-Log-File -name $workbook -folder "Workbooks") -message "Report-Trigger: $workbook - Beginning process"
            if (Update-Tableau-Workbook -workbook $workbook -timestamp (Get-Date)) {
                Log-Message -filepath (Get-Log-File -name $workbook -folder "Workbooks") -message "Report-Trigger: $workbook - Sending emails"
                Send-Reports
                Log-Message -filepath (Get-Log-File -name $workbook -folder "Workbooks") -message "Report-Trigger: $workbook - Finished process"
            }
        } Catch {
            Handle-Error -message "Report-Trigger: $workbook - Failed with: $error." -name $workbook -folder "Workbooks" 
        }
    }
}

Report-Trigger -workbook $workbook 