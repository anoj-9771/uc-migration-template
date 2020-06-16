 <#
    Runbook Tasks:
    Install-Module AzureAutomationAuthoringToolkit
    Import-Module AzureAutomationAuthoringToolkit

    Step1:
        Create AD App and assign it required role permissions in Azure Storage and Key Vault
    Step2:
        Automation Asset Variables will need to be added for
            RotateKeys-ClientId = Application ID(client)
            RotateKeys-ClientSecret = Application Secret
            DirectoryDomainName = e.g. ignia.com.au
            SubscriptionId = Subscription ID
            RotateKeys-StorageAccountKeyVaultMapping = json string in following format
                { 
                    "keyVault": [
                        {
                            "name": "<keyVaultName>",    
                            "storageAcc": [
                                {
                                    "name": "<storageAccountName>",
                                    "container":[
                                        {
                                            "name": "<containerName>",
                                            "permission": "<rwdl;rw;>"
                                            "secret": "<secretName>"
                                        }
                                    ]
                                }
                            ]
                        }
                    ]
                }

#> 
param([int]$TokenDuration = 46080) #Default set in minutes to 32 Days

# $ErrorActionPreference = "Stop"

$apiVersion = "2017-06-01"
$vaultApiVersion = "2015-06-01"

$clientId = Get-AutomationVariable -Name "RotateKeys-ClientId"
$clientSecret = Get-AutomationVariable -Name "RotateKeys-ClientSecret"
$directoryDomainName = Get-AutomationVariable -Name "DirectoryDomainName"
$subscriptionId = Get-AutomationVariable -Name "SubscriptionId"
$storageAccountKeyVaultMapping = Get-AutomationVariable -Name "RotateKeys-StorageAccountKeyVaultMapping"

function ConvertTo-Hashtable {
    [CmdletBinding()]
    [OutputType('hashtable')]
    param (
        [Parameter(ValueFromPipeline)]
        $InputObject
    )
 
    process {
        ## Return null if the input is null. This can happen when calling the function
        ## recursively and a property is null
        if ($null -eq $InputObject) {
            return $null
        }
 
        ## Check if the input is an array or collection. If so, we also need to convert
        ## those types into hash tables as well. This function will convert all child
        ## objects into hash tables (if applicable)
        if ($InputObject -is [System.Collections.IEnumerable] -and $InputObject -isnot [string]) {
            $collection = @(
                foreach ($object in $InputObject) {
                    ConvertTo-Hashtable -InputObject $object
                }
            )
 
            ## Return the array but don't enumerate it because the object may be pretty complex
            Write-Output -NoEnumerate $collection
        } elseif ($InputObject -is [psobject]) { ## If the object has properties that need enumeration
            ## Convert it to its own hash table and return it
            $hash = @{}
            foreach ($property in $InputObject.PSObject.Properties) {
                $hash[$property.Name] = ConvertTo-Hashtable -InputObject $property.Value
            }
            $hash
        } else {
            ## If the object isn't an array, collection, or other object, it's already a hash table
            ## So just return it.
            $InputObject
        }
    }
}

function Get-ResponseError {
    param([System.Net.HttpWebResponse]$Response)
    $streamReader = New-Object System.IO.StreamReader($Response.GetResponseStream())
    $streamReader.BaseStream.Position = 0
    $streamReader.DiscardBufferedData()
    $body = ConvertFrom-Json($streamReader.ReadToEnd())
    Add-Member -InputObject $body -MemberType NoteProperty -Name "StatusCode" -Value $_.Exception.Response.StatusCode
    $body
}

function Invoke-GetRequest {
    param([String]$Uri, [hashtable]$Headers)
    $response = try {
        Invoke-RestMethod -Method GET -Uri $Uri -Headers $Headers
    } catch {
        Get-ResponseError $_.Exception.Response
    }

    return $response
}

function Invoke-PostRequest {
    param([String]$Uri, [hashtable]$Headers, [String]$Body)
    $response = try {
    Invoke-RestMethod -Method POST -Uri $Uri -Headers $Headers -Body $Body
    } catch {
        Get-ResponseError $_.Exception.Response
    }

    return $response
}

function Invoke-PutRequest {
    param([String]$Uri, [hashtable]$Headers, [String]$Body)
    $response = try {
    Invoke-RestMethod -Method PUT -Uri $Uri -Headers $Headers -Body $Body
    } catch {
        Get-ResponseError $_.Exception.Response
    }

    return $response
}

## Acquire token from Azure AD for Azure Resource Manager using https://management.core.windows.net/
function Get-AzureRMTokenFromAD {
    $uri = "https://login.windows.net/$directoryDomainName/oauth2/token"
    $body = "grant_type=client_credentials"
    $body += "&client_id=$clientId"
    $body += "&client_secret=$([Uri]::EscapeDataString($clientSecret))"
    $body += "&resource=$([Uri]::EscapeDataString("https://management.core.windows.net/"))"
    $enc = New-Object System.Text.ASCIIEncoding
    $byteArray = $enc.GetBytes($body)
    $contentLength = $byteArray.Length
    $headers = @{
        "Accept" = "application/json";
        "Content-Type" = "application/x-www-form-urlencoded";
        "Content-Length" = $contentLength;
    }
    $result = Invoke-PostRequest $uri $headers $body

    if ($result.access_token -eq $null) {
    Write-Error "Failed to get a token from AD for the Resource Manager because the request failed with status $($result.StatusCode) and message $($result.error.message)"
    }
    write-Host $result.access_token
    return $result.access_token
}

## Acquire token from Azure AD for Azure Key Vault using https://vault.azure.net
function Get-KeyVaultTokenFromAD {
    $uri = "https://login.windows.net/$directoryDomainName/oauth2/token"
    $body = "grant_type=client_credentials"
    $body += "&client_id=$clientId"
    $body += "&client_secret=$([Uri]::EscapeDataString($clientSecret))"
    $body += "&resource=$([Uri]::EscapeDataString("https://vault.azure.net"))"
    $enc = New-Object System.Text.ASCIIEncoding
    $byteArray = $enc.GetBytes($body)
    $contentLength = $byteArray.Length
    $headers = @{
        "Accept" = "application/json";
        "Content-Type" = "application/x-www-form-urlencoded";
        "Content-Length" = $contentLength;
    }
    $result = Invoke-PostRequest $uri $headers $body

    if ($result.access_token -eq $null) {
    Write-Error "Failed to get a token from AD for the Key Vault because the request failed with status $($result.StatusCode) and message $($result.error.message)"
    }

    return $result.access_token
}

  
## Parse the storageAccountKeyVaultMapping variable into a hashtable of storage account names and keyvault names
function Get-StorageAccountAndKeyVaultNames {
    param($componentMap, [System.Collections.ArrayList]$StorageAccounts)
    $componentMap.keyVault| %{
        $kv = $_
        $kv.storageAcc| %{
            [void]$StorageAccounts.Add($_.name.ToLower())
        }
    }
}

## Query Azure Resource Manager reosurces API to get the storage accounts.
## Query both, Microsoft.ClassicStorage as well as Microsoft.Storage namespaces
function Get-StorageAccountObjects {
    param([String]$ArmAccessToken, [System.Collections.ArrayList]$StorageAccounts)
    $storageAccountsSearchSubFilter = Get-StorageAccountsFilter($StorageAccounts)
    $uri = "https://management.azure.com/subscriptions/$($subscriptionId)/resources?api-version=$($apiVersion)&`$filter=(resourceType eq 'Microsoft.ClassicStorage/storageAccounts' or resourceType eq 'Microsoft.Storage/storageAccounts') and ($($storageAccountsSearchSubFilter))"
    $headers = @{
    "Authorization" = "Bearer $($ArmAccessToken)";
    "Content-Type" = "application/json";
    }
    $result = Invoke-GetRequest $uri $headers
    if ($result.value -eq $null) {
    Write-Error "Failed to get the Storage Account objects because the request failed with status $($result.StatusCode) and message $($result.error.message)"
    }
    return $result.value
}

function Get-StorageAccountsFilter {
    param([System.Collections.ArrayList]$StorageAccounts)
    $storageAccountsSearchSubFilter = ""
    $count = 0

    $StorageAccounts.keyVault.storageAcc | % {
    $storageAccountsSearchSubFilter += "substringof('$($_.name)', Name)"
    if ($count -lt $StorageAccounts.Count - 1) {
        $storageAccountsSearchSubFilter += " or "
    }
    $count++
    }

    return $storageAccountsSearchSubFilter
}

# Generate SAS Token on Storage Account with Read and List Permissions
function Generate-SASToken {
    param([String]$StorageAccountName, [String]$StorageAccountKey, [String]$ContainerName, [String]$Permission)
    $context = New-AzureStorageContext -StorageAccountName $StorageAccountName -StorageAccountKey $StorageAccountKey
    $startTime = (Get-Date)
    $expiryTime= (Get-Date).AddMinutes($TokenDuration)
    $newSASTokenURI = $context | New-AzureStorageContainerSASToken -Name $ContainerName -StartTime $startTime -ExpiryTime $expiryTime -Permission $Permission
    return [System.Web.HttpUtility]::UrlEncode($newSASTokenURI.Substring(1)) # Starts with ? so strip off.
}

##Write new SAS Token to the Key Vault by appending -read-only-sas-token
function Write-SASTokenToKeyVault {
    param([System.Collections.Hashtable]$componentMap, [String]$StorageAccountName, [String]$ContainerName, [String]$SecretName, [String]$EncodedSASToken, [String]$KVAccessToken)
    $keyVault = $componentMap.keyVault.name | where-object {$componentMap.keyVault.storageAcc.name -eq $StorageAccountName}
    $uri = "https://$($keyVault).vault.azure.net/secrets/$($SecretName)?api-version=$($vaultApiVersion)"
    $postData = "" | select value
    $postData.value = $EncodedSASToken
    $enc = New-Object "System.Text.ASCIIEncoding"
    $body = ConvertTo-Json $postData
    $byteArray = $enc.GetBytes($body)
    $contentLength = $byteArray.Length
    $headers = @{
    "Authorization" = "Bearer $KVAccessToken";
    "Content-Type" = "application/json";
    "Content-Length" = $contentLength;
    }
    $result = Invoke-PutRequest $uri $headers $body

    if ($result.value -eq $null) {
    Write-Error "Failed to write the SAS token for Storage Account $($StorageAccountName) into Key Vault $($keyVault) because the request failed with status $($result.StatusCode) and message $($result.error.message)"
    }

    Write-Output "Generated SAS Token for Storage Account $($StorageAccountName) and saved it in Key Vault $($keyVault) as Secret $($result.id)"
}

function Write-StorageAccountSAS {
    param([String]$ArmAccessToken, [System.Collections.Hashtable]$componentMap, [PSCustomObject]$StorageAccountObject, [String]$ContainerName, [String]$SecretName, [String]$Permission, [String]$KVAccessToken)
    if ($storageAccounts.Contains($storageAccountObject.name)) {
        $uri = "https://management.azure.com$($StorageAccountObject.id)/listKeys?api-version=$($apiVersion)"
        $headers = @{"Authorization" = "Bearer $ArmAccessToken";}
        $result = Invoke-PostRequest $uri $headers ""

    if ($result.keys -eq $null) {
        Write-Error "Failed to get a token from AD for the Resource Manager because the request failed with status $($result.StatusCode) and message $($result.error.message)"
    }

    if ($result.keys[0].value -eq $null) {
        Write-Error "Invalid response returned when getting storage account keys. Response was $($result)"
    }

    $encodedSASToken = Generate-SASToken $storageAccountObject.name $result.keys[0].value $ContainerName $Permission
    if ($encodedSASToken  -eq $null) {
        Write-Error "Failed to generate a SAS Token for Container $($ContainerName) in Storage Account $($StorageAccountName)"
    }

    Write-SASTokenToKeyVault $componentMap $StorageAccountObject.name $ContainerName $SecretName $encodedSASToken $KVAccessToken
    }
}

Write-Output "Getting a token from Active Directory for the Resource Manager"
$armAccessToken = Get-AzureRMTokenFromAD
Write-Output "Successfully got a token from Active Directory for the Resource Manager"

Write-Output "Getting a token from Active Directory for the Key Vault"
$kvAccessToken = Get-KeyVaultTokenFromAD $armAccessToken
Write-Output "Successfully got a token from Active Directory for the Key Vault"

Write-Output "Getting the Storage Account and Key Vault names"
$componentMap = $storageAccountKeyVaultMapping | ConvertFrom-Json | ConvertTo-HashTable   
[System.Collections.ArrayList]$storageAccounts = @()
Get-StorageAccountAndKeyVaultNames $componentMap $storageAccounts 
Write-Output "Got the Storage Account and Key Vault names"

Write-Output "Getting the Storage Account objects"
$storageAccountObjects = Get-StorageAccountObjects $armAccessToken $storageAccounts
Write-Output "Successfully got the Storage Account objects"

Write-Output "Writing the SAS tokens to the Key Vault"
$storageAccountObjects | % { 
    $sao = $_
    $componentMap.keyVault.storageAcc |%{
        $sa = $_.name
        $_.container |%{
            $cn = $_.name | where-object {$sa -eq $sao.name}
            $pn = $_.permission | where-object {$sa -eq $sao.name}
            if (!$_.secret){
                $sn = "$($sa)-$($_.name)-$($pn)-SaSToken"
            }
            else{
                $sn = $_.secret | where-object {$sa -eq $sao.name}
            }
            Write-StorageAccountSAS $armAccessToken $componentMap $sao $cn $sn $pn $kvAccessToken
        }
    }
}


