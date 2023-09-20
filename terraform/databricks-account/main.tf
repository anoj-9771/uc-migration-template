module "dev_uc_components" {
  source                  = "./module"
  env                     = "dev_"
  container_name          = "dev"
  catalog_storage_account = "sadafdev02"
  storage_account         = "sadafdev01"
  admin_group             = "swc-metastore-admin"
  managed_identity_name   = "id-dbw-swcnonprod01-daf-data-dev-01"
  access_connector_id     = "/subscriptions/e59a4313-66c3-4db6-842d-6154c5e08205/resourceGroups/rg-swcnonprod01-daf-dev-01/providers/Microsoft.Databricks/accessConnectors/id-dbw-swcnonprod01-daf-data-dev-01"
  resource_group          = "rg-swcnonprod01-dev-daf-01"
  db_service_principal    = "acff96db-8630-433b-bbb1-35a3813fa036"
  adf_service_principal   = "407e7ff2-525f-4d47-90f9-1c22fbfd977f"
  digital_ad_groups       = ["A-Azure-rg-swcnonprod01-dev-daf-01-DataAdmin", "A-Azure-rg-swcnonprod01-dev-daf-01-DataDeveloper"]
  service_principals      = ["407e7ff2-525f-4d47-90f9-1c22fbfd977f", "acff96db-8630-433b-bbb1-35a3813fa036"]
  data_developer_grant    = { included = "yes" }
  databricks_host         = "https://adb-7004525605760210.10.azuredatabricks.net/"
}

module "test_uc_components" {
  source                  = "./module"
  env                     = "test_"
  container_name          = "test"
  catalog_storage_account = "sadaftest02"
  storage_account         = "sadaftest01"
  admin_group             = "swc-metastore-admin"
  managed_identity_name   = "id-dbw-swcnonprod01-daf-data-test-01"
  access_connector_id     = "/subscriptions/e59a4313-66c3-4db6-842d-6154c5e08205/resourceGroups/rg-swcnonprod01-daf-test-01/providers/Microsoft.Databricks/accessConnectors/id-dbw-swcnonprod01-daf-data-test-01"
  resource_group          = "rg-swcnonprod01-test-daf-01"
  db_service_principal    = "13daf0ce-c685-41b1-b609-0747c83bc4ac"
  adf_service_principal   = "d47f1828-3216-400f-85f8-49320a296492"
  digital_ad_groups       = ["A-Azure-rg-swcnonprod01-test-daf-01-DataAdmin", "A-Azure-rg-swcnonprod01-test-daf-01-DataDeveloper"]
  service_principals      = ["d47f1828-3216-400f-85f8-49320a296492", "13daf0ce-c685-41b1-b609-0747c83bc4ac"]
  data_developer_grant    = { included = "yes" }
  databricks_host         = "https://adb-1108007146617792.12.azuredatabricks.net/"
}


module "preprod_uc_components" {
  source                  = "./module"
  env                     = "ppd_"
  container_name          = "preprod"
  catalog_storage_account = "sadafpreprod02"
  storage_account         = "sadafpreprod01"
  admin_group             = "swc-metastore-admin"
  managed_identity_name   = "id-dbw-swcprod01-daf-data-preprod-01"
  access_connector_id     = "/subscriptions/769a62b3-7481-44ae-878e-a466593f50ac/resourceGroups/rg-swcprod01-preprod-daf-01/providers/Microsoft.Databricks/accessConnectors/id-dbw-swcprod01-daf-data-preprod-01"
  resource_group          = "rg-swcprod01-preprod-daf-01"
  db_service_principal    = "3012901b-9b8c-4100-a6bf-a1a2ec010def"
  adf_service_principal   = "e3d4b951-6cbb-4ab4-aaf6-0c5ee7dc5a25"
  digital_ad_groups       = ["A-Azure-rg-swcprod01-preprod-daf-01-DataAdmin", "A-Azure-rg-swcprod01-preprod-daf-01-DataDeveloper"]
  service_principals      = ["e3d4b951-6cbb-4ab4-aaf6-0c5ee7dc5a25", "3012901b-9b8c-4100-a6bf-a1a2ec010def"]
}

module "prod_uc_components" {
  source                  = "./module"
  env                     = ""
  container_name          = "prod"
  catalog_storage_account = "sadafprod02"
  storage_account         = "sadafprod01"
  admin_group             = "swc-metastore-admin"
  managed_identity_name   = "id-dbw-swcprod01-daf-data-prod-01"
  access_connector_id     = "/subscriptions/769a62b3-7481-44ae-878e-a466593f50ac/resourceGroups/rg-swcprod01-prod-daf-01/providers/Microsoft.Databricks/accessConnectors/id-dbw-swcprod01-daf-data-prod-01"
  create_one_off          = true
  resource_group          = "rg-swcprod01-prod-daf-01"
  db_service_principal    = "07ffd3b5-2923-4df9-aa50-8525c7a36bad"
  adf_service_principal   = "1af18bc3-a05d-423a-8f85-0d5ff002d19d"
  digital_ad_groups       = ["A-Azure-rg-swcprod01-prod-daf-01-DataAdmin", "A-Azure-rg-swcprod01-prod-daf-01-DataDeveloper"]
  service_principals      = ["1af18bc3-a05d-423a-8f85-0d5ff002d19d", "07ffd3b5-2923-4df9-aa50-8525c7a36bad"]
}

terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "=3.0.0"
    }
    databricks = {
      source = "databricks/databricks"
    }
  }
  backend "azurerm" {
    resource_group_name  = "rg-swcnonprod01-daf-dev-01"
    storage_account_name = "sadafdev01"
    container_name       = "tfstate"
    key                  = "databricks-account-terraform.tfstate"
    subscription_id      = "e59a4313-66c3-4db6-842d-6154c5e08205"
    client_id            = "acff96db-8630-433b-bbb1-35a3813fa036"
    tenant_id            = "8351bb5c-749d-4ee4-b1c4-71a3971acbe9"
  }
}

provider "azurerm" {
  features {}
}

provider "databricks" {
  host = var.databricks_host
}
