param (
    [parameter(Mandatory = $false)] [string]$dwName,
    [parameter(Mandatory = $false)] [Bool] $predeployment=$true
)

$dwDetail = (Get-AzureRMResource | Where-Object {$_.Kind -like "*datawarehouse*" -and $_.Name -like "*/$dwName"})
if ($dwDetail) {
	$dwDetail = $dwDetail.ResourceId.Split("/")
	$resourceGroup = $dwDetail[4]
	$serverName = $dwDetail[8]
	$databaseName = $dwDetail[10]

	"Resource Group: $resourceGroup"
	"Server Name: $serverName"
	"Database Name: $databaseName"

	$database = (Get-AzureRMSqlDatabase -ResourceGroup $resourceGroup -ServerName $serverName -DatabaseName $databaseName)
	$dwStatus = $database.Status

    $dwSemiStates = "Resuming", "Pausing"
    $dwEndStates = "Paused", "Online"

    #wait in 5 second intervals until DW status is no longer pausing or resuming
	if ($dwSemiStates.Contains($dwStatus) -eq $true) {
        $i = 0
        do {
			$database = (Get-AzureRMSqlDatabase -ResourceGroup $resourceGroup -ServerName $serverName -DatabaseName $databaseName)
			$dwStatus = $database.Status

			if ($dwEndStates.Contains($dwStatus) -eq $true) {
				$i = 1
			}
             
			"Waiting..."
			Start-Sleep -Seconds 5
        } while($i -ne 1)
    }

	if ($predeployment -eq $true) {
		# Save State into pipeline variable
		Write-Host "##vso[task.setvariable variable=previousState;]$dwStatus"
		"Current DW State: $dwStatus"
			
		# DW is currently paused, resume for deployment
		if ($dwStatus -eq "Paused") {
			"Resuming DW..."
			$database | Resume-AzureRMSqlDatabase
		}
	}
	else {
		# Get DW State from pipeline variable
		$previousState = $env:PREVIOUSSTATE
		"Previous DW State: $previousState"

		# DW was previously paused, pause after deployment
		if ($previousState -eq "Paused") {
			"Suspending DW..."
			$database | Suspend-AzureRMSqlDatabase
		}
	}
}
else {
	"Cannot find Azure DW $dwName."
}
