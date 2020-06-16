
$objectName = ("CBHARZ-INF-SAND-SVC-ADO","CBHARZ-INF-NPE-SVC-ADO", "CBHARZ-INF-PRD-SVC-ADO")
$rgName = ("cbhazr-inf-sand-rg", "cbhazr-inf-npe-rg", "cbhazr-inf-prd-rg") 
for ($i=0; $i -lt $rgName.length; $i++){
    $sp = az ad sp create-for-rbac -n $objectName[$i] --years 100|convertFrom-Json 
    $id = az ad sp show --id $sp.appid | convertFrom-Json
    az group create -n $rgName[$i] --location australiaeast
    az role assignment create --role Contributor --assignee-object-id $id.objectId --resource-group $rgName[$i]
    $sp.password
}
