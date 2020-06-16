
<#
 .SYNOPSIS
    Deploys a template to Azure

 .DESCRIPTION
    Deploys an Azure Resource Manager template

 .PARAMETER Environment
    The environment for which the resources will be used. Valid values are PROD, TEST & DEV.

 .PARAMETER subscriptionId
    The subscription id where the template will be deployed.

 .PARAMETER resourceGroupName
    The resource group where the template will be deployed. Can be the name of an existing or a new resource group.

 .PARAMETER resourceGroupLocation
    The resource group location. If specified, will try to create a new resource group in this location.

 .PARAMETER deploymentName
    Optional, The deployment name.

 .PARAMETER supportFilePath
    Path to the template file and template parameter files. Defaults to template.json.

 .PARAMETER templateFileName
    The name of the .json file containing the deployment template details.

 .PARAMETER testParametersFileName
    The name of the .json parameters file containing the deployment parameters for the TEST environment.

 .PARAMETER devParametersFileName
    The name of the .json parameters file containing the deployment parameters for the DEV environment.

 .PARAMETER prodParametersFileName
    The name of the .json parameters file containing the deployment parameters for the PROD environment.
#>


param(

 [Parameter(Mandatory=$True)]
 [ValidateSet('PRD','NPE')]
 [string]
 $Environment,

 [Parameter(Mandatory=$True)]
 [ValidateSet('Azure','ADF')]
 [string]
 $Resource,
  
 [string]
 $subscriptionId = "",

 [string]
 $resourceGroupName = "",

 [string]
 $resourceGroupLocation = "",

 [string]
 $deploymentName ="",

 [string]
 $templateFileName = "",

 [string]
 $parametersFileName = ""
)

$configPath = [System.IO.Path]::GetFullPath([System.IO.Path]::Combine($PSScriptRoot, "AzureConfig.json"))
$config = Get-Content -Path $configPath -Raw |
  ConvertFrom-Json

<#
.SYNOPSIS
    Registers RPs
#>
try {
    [Microsoft.Azure.Common.Authentication.AzureSession]::ClientFactory.AddUserAgent("VSAzureTools-$UI$($host.name)".replace(' ','_'), '3.0.0')
} catch { }

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version 3

function Format-ValidationOutput {
    param ($ValidationOutput, [int] $Depth = 0)
    Set-StrictMode -Off
    return @($ValidationOutput | Where-Object { $_ -ne $null } | ForEach-Object { @('  ' * $Depth + ': ' + $_.Message) + @(Format-ValidationOutput @($_.Details) ($Depth + 1)) })
}

Function RegisterRP {
    Param(
        [string]$ResourceProviderNamespace
    )

    Write-Host "Registering resource provider '$ResourceProviderNamespace'";
    Register-AzureRmResourceProvider -ProviderNamespace $ResourceProviderNamespace;
}

#******************************************************************************
# Script body
# Execution begins here
#******************************************************************************
$ErrorActionPreference = "Stop"

$parametersFilePath = "";
$templateFilePath = [System.IO.Path]::GetFullPath([System.IO.Path]::Combine($PSScriptRoot, $config.deployment.template))



Switch ($Environment) 
{
    "NPE" {    
        $subscriptionId = $config.deployment.environment.npe.subscription.id
        $resourceGroupName = $config.deployment.environment.npe.subscription.resourceGroup.name
        $resourceGroupLocation = $config.deployment.environment.npe.subscription.resourceGroup.location
        Switch ($Resource) 
        {
            "Azure" {
                $parametersFilePath = [System.IO.Path]::GetFullPath([System.IO.Path]::Combine($PSScriptRoot, $config.deployment.environment.npe.parameters.azureResources));
            }
            "ADF" {
                $parametersFilePath = [System.IO.Path]::GetFullPath([System.IO.Path]::Combine($PSScriptRoot, $config.deployment.environment.npe.parameters.adfResources));
            }
        }
    }
    "PRD" {   
        $subscriptionId = $config.deployment.environment.prd.subscription.id
        $resourceGroupName = $config.deployment.environment.prd.subscription.resourceGroup.name
        $resourceGroupLocation = $config.deployment.environment.prd.subscription.resourceGroup.location
        Switch ($Resource) 
        {
            "Azure" {
                $parametersFilePath = [System.IO.Path]::GetFullPath([System.IO.Path]::Combine($PSScriptRoot, $config.deployment.environment.prd.parameters.azureResources));
            }
            "ADF" {
                $parametersFilePath = [System.IO.Path]::GetFullPath([System.IO.Path]::Combine($PSScriptRoot, $config.deployment.environment.prd.parameters.adfResources));
            }
        }
    }
}


Import-Module AzureRM.Resources

# sign in
Write-Host "Logging in...";
#Login-AzureRmAccount;

# select subscription
Write-Host "Selecting subscription '$subscriptionId'";
Select-AzureRmSubscription -SubscriptionID $subscriptionId;

#Create Optional Paramaters
#$OptionalParameters = New-Object -TypeName Hashtable;



#Create or check for existing resource group
$resourceGroup = Get-AzureRmResourceGroup -Name $resourceGroupName -ErrorAction SilentlyContinue
if(!$resourceGroup)
{
    Write-Host "Resource group '$resourceGroupName' does not exist.";
    if(!$resourceGroupLocation) {
        $resourceGroupLocation = Read-Host "resourceGroupLocation";
    }
    Write-Host "Creating resource group '$resourceGroupName' in location '$resourceGroupLocation'";
    New-AzureRmResourceGroup -Name $resourceGroupName -Location $resourceGroupLocation
}
else{
    Write-Host "Using existing resource group '$resourceGroupName'";
}

# Start the deployment
Write-Host "Starting deployment...";
if(Test-Path $parametersFilePath) {
     New-AzureRmResourceGroupDeployment -Name ((Get-ChildItem $templateFilePath).BaseName + '-' + ((Get-Date).ToUniversalTime()).ToString('MMdd-HHmm')) `
                                       -ResourceGroupName $resourceGroupName `
                                       -TemplateFile $templateFilePath `
                                       -TemplateParameterFile $parametersFilePath `
                                       -Force -Verbose `
} 

#Set Keyvault diagnositcs to 
#Set-AzureRMDataFactoryV2 -ResourceGroupName $resourceGroupName -Name cbhazr-inf-datafactory-sand -Location "Australia East"
#Get-AzureRMADUser
#Get-AzureRMResource -ResourceGroupName $resourceGroupName -Name cbhazr-inf-keyvault-sand
#azureRM vm list-usage --location "Australia East"