param (
    [string]$storageNameParam = "",
    [string]$sourceDirectory = "$(Build.SourcesDirectory)",
    [string]$outputDirectory = "$(Build.ArtifactStagingDirectory)"
)

"Copying asdatabase..."
   
$dbServerToken = "(dbServer_token)"
$dbUserToken = "(dbUser_token)"
$dbPasswordToken = "(dbPw_token)"
$dbNameToken = "(dbName_token)"
$dwNameToken = "(dwName_token)"
$storageNameToken = "(storageName_token)"
$storageName = $storageNameParam

$i = 0
Get-ChildItem -Filter "bin" -Recurse -Path $sourceDirectory -Directory | 
Get-ChildItem -recurse -filter *.asdatabase -file | ForEach-Object {
    $i++

    $file = Get-Content $_.fullname -raw | ConvertFrom-Json  
        if (-not($file.model.dataSources)) {
            # Write out updated file
            $file | Set-Content "$outputDirectory/model$i.asdatabase" -encoding utf8
        } else {
            $file.model.dataSources | ForEach-Object {
                # Skip if list is empty

                # Build Azure SQL ConnectionString
                if ($_.name -eq "SQL") {
                    $_.connectionString = "Data Source=$dbServerToken;Initial Catalog=$dbNameToken;User ID=$dbUserToken;password=$dbPasswordToken;Persist Security Info=false;Encrypt=true;TrustServerCertificate=false"
                }
                # Build Azure DW ConnectionString
                if ($_.name -eq "ADW") {
                    $_.connectionString = "Data Source=$dbServerToken;Initial Catalog=$dwNameToken;User ID=$dbUserToken;password=$dbPasswordToken;Persist Security Info=false;Encrypt=true;TrustServerCertificate=false"
                }
                # Add Blob Key to credential object
                if ($_.name.StartsWith("AzureBlobs/")) {           
                    if ($storageName -eq "") {
                            # If storageName not supplied, try to read from file
                        $storageName = $_.connectionDetails.address.account
                    }
                }
            }
            # Replace storageName throughout model
            if ($storageName -ne "") {
                $file = $file.replace($storageName, $storageNameToken)
            }

            # Write out updated file
            $file | Set-Content "$outputDirectory/model$i.asdatabase" -encoding utf8
        
    

    
            $file = $file | ConvertTo-Json -Depth 32

            # Replace storageName throughout model
            if ($storageName -ne "") {
                $file = $file.replace($storageName, $storageNameToken)
            }

            # Write out updated file
            $file | Set-Content "$outputDirectory/model$i.asdatabase" -encoding utf8
        }
    
}

"Copying deploymentoptions..."


$i = 0
Get-ChildItem -Filter "bin" -Recurse -Path $sourceDirectory  -Directory | 
Get-ChildItem -recurse -filter *.deploymentoptions -file | ForEach-Object {
    $i++
    $xml = [xml](Get-Content $_.fullname)
    $xml = [xml](Get-Content $_.fullname)
    $xml.SelectNodes("//PartitionDeployment") | ForEach-Object { 
        $_."#text" = "RetainPartitions"
    }
    $xml.SelectNodes("//RoleDeployment") | ForEach-Object { 
        $_."#text" = "DeployRolesRetainMembers"
    }
    $xml.SelectNodes("//ProcessingOption") | ForEach-Object { 
        $_."#text" = "DoNotProcess"
    }
    
    $xml.Save("$outputDirectory/model$i.deploymentoptions")

        #copy-item $_.fullname "$outputDirectory/model$i.deploymentoptions"
#    }
}

"Copying deploymenttargets.."

$asServerToken = "(asServer_token)"
$asUserToken = "(asUser_token)"
$asPwToken = "(asPw_token)"

$i = 0
Get-ChildItem -Filter "bin" -Recurse -Path $sourceDirectory -Directory | 
Get-ChildItem -recurse -filter *.deploymenttargets -file | ForEach-Object {
    $i++
    $xml = [xml](Get-Content $_.fullname)
    $xml.SelectNodes("//Server") | ForEach-Object { 
        $_."#text" = $asServerToken
    }
    $xml.SelectNodes("//ConnectionString") | ForEach-Object { 
        $_."#text" = "DataSource=$asServerToken;Timeout=0;UID=$asUserToken;Password=$asPwToken"
    }
    $xml.Save("$outputDirectory/model$i.deploymenttargets")
}
