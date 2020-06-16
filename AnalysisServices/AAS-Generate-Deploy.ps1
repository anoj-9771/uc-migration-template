param (
    [string]$tenantId, 
    [string]$servicePrincipalId,
    [string]$sourcePath,
    [string]$asServer, 
    [string]$asUser, 
    [string]$asPw,
    [string]$dbServer, 
    [string]$dbUser, 
    [string]$dbPw, 
    [string]$dbName, 
    [string]$dwName,
    [string]$storageName,
    [string]$storageKey,
    [string]$deploymentWizardPath = "C:\Program Files (x86)\Microsoft SQL Server Management Studio 18\Common7\IDE"
)

$buildPath = $sourcePath + "\buildoutput\"
$deploymentWizard =  $deploymentWizardPath + '\Microsoft.AnalysisServices.Deployment.exe'
New-Item -ItemType Directory -Force -Path $buildPath

# Retrieve AAS Server
$asServerName = $asServer.Substring($asServer.LastIndexOf("/") + 1)
$asServerObject = Get-AzureRMAnalysisServicesServer -Name $asServerName
$asServerOriginalState = $asServerObject.State

"AAS Server Name: $asServerName"
"AAS Current State: $asServerOriginalState"

# Resume AAS if it's currently paused
if($asServerObject.State -eq "Paused")
{
    "Resuming..."
    $asServerObject | Resume-AzureRMAnalysisServicesServer
}

<# REPLACING TOKENS #>
Get-ChildItem -Filter "drop" -Recurse -Path $sourcePath -Directory | 
Get-ChildItem -recurse -filter *.deploymenttargets -file | ForEach-Object {
    ((Get-Content -path $_.fullname -Raw) -replace '[(]asServer_token[)]', $asServer) | Set-Content -Path $_.fullname -Encoding utf8
    ((Get-Content -path $_.fullname -Raw) -replace '[(]asUser_token[)]', $asUser) | Set-Content -Path $_.fullname -Encoding utf8
    ((Get-Content -path $_.fullname -Raw) -replace '[(]asPw_token[)]', $asPw) | Set-Content -Path $_.fullname -Encoding utf8
}

Get-ChildItem -Filter "drop" -Recurse -Path $sourcePath -Directory | 
Get-ChildItem -recurse -filter *.asdatabase -file | ForEach-Object {
    ((Get-Content -path $_.fullname -Raw) -replace '[(]dbServer_token[)]', $dbServer) | Set-Content -Path $_.fullname -Encoding utf8
    ((Get-Content -path $_.fullname -Raw) -replace '[(]dbUser_token[)]', $dbUser) | Set-Content -Path $_.fullname -Encoding utf8
    ((Get-Content -path $_.fullname -Raw) -replace '[(]dbPw_token[)]', $dbPw) | Set-Content -Path $_.fullname -Encoding utf8
    ((Get-Content -path $_.fullname -Raw) -replace '[(]dbName_token[)]', $dbName) | Set-Content -Path $_.fullname -Encoding utf8
    ((Get-Content -path $_.fullname -Raw) -replace '[(]dwName_token[)]', $dwName) | Set-Content -Path $_.fullname -Encoding utf8
    ((Get-Content -path $_.fullname -Raw) -replace '[(]storageName_token[)]', $storageName) | Set-Content -Path $_.fullname -Encoding utf8
}

<# BUILD XMLA #>
Get-ChildItem -Filter "drop" -Recurse -Path $sourcePath -Directory | 
Get-ChildItem -recurse -filter *.asdatabase -file | ForEach-Object {
    $filename = $_.fullname
    $generatedFile = $buildPath + $_.BaseName + ".xmla"
    "Processing $filename"
    & $deploymentWizard $filename /o:$generatedFile

    # Have to add Blob Key now, as Deployment Wizard doesn't like adding the Key (bug maybe?)
    $file = Get-Content $generatedFile -Raw | ConvertFrom-Json  
        if (-not($file.createOrReplace.database.model.dataSources)) {
            $file = $file | ConvertTo-Json -Depth 32
            $file | Set-Content -Path $generatedFile -Encoding utf8
        
        } else { 
         
            $file.createOrReplace.database.model.dataSources | ForEach-Object {
                # Add Blob Key to credential object
                if ($_.name.StartsWith("AzureBlobs/")) {           
                    $_.credential | Add-Member -Name "Key" -Value $storageKey -MemberType NoteProperty -Force
                }
            }
        
            $file = $file | ConvertTo-Json -Depth 32
            $file | Set-Content -Path $generatedFile -Encoding utf8
        }
    # Add DB User & Password back into XMLA file
    ((Get-Content -path $generatedFile -Raw) -replace 'User ID=;', "User ID=$dbUser;") | Set-Content -Path $generatedFile -Encoding utf8
    ((Get-Content -path $generatedFile -Raw) -replace 'Password=;', "Password=$dbPw;") | Set-Content -Path $generatedFile -Encoding utf8
    $?
}

install-module Azure.AnalysisServices -scope currentuser -force -allowclobber

$password = ConvertTo-SecureString $asPw -AsPlainText -Force
$cred = New-Object System.Management.Automation.PSCredential ($servicePrincipalId, $password)
Add-AzureAnalysisServicesAccount -Credential $cred -ServicePrincipal -TenantId $tenantId -RolloutEnvironment 'australiaeast.asazure.windows.net'

install-module sqlserver -scope currentuser -force -AllowClobber

<# DEPLOY XMLA #>
Get-ChildItem $buildPath | ForEach-Object{ 
    "Deploying " + $_.Name 
    Invoke-ASCmd -InputFile $_.FullName -Server $asServer
    $?
}

# Restore AAS to previous state
if($asServerOriginalState -eq "Paused") {
    "Pausing..."
    $asServerObject | Suspend-AzureRMAnalysisServicesServer
}

$?
"Deployment completed successfully!"
