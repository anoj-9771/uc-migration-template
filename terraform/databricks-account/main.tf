module "dev_uc_components" {
  source                  = "./module"
  env                     = "dev_"
  container_name          = "dev"
  catalog_storage_account = ""
  storage_account         = ""
  admin_group             = "companyx-metastore-admin"
  managed_identity_name   = ""
  access_connector_id     = ""
  resource_group          = ""
  db_service_principal    = ""
  adf_service_principal   = ""
  digital_ad_groups       = [""]
  service_principals      = [""]
  data_developer_grant    = { included = "yes" }
  databricks_host         = ""
}

module "test_uc_components" {
...
}


module "preprod_uc_components" {
...
}

module "prod_uc_components" {
...
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
    resource_group_name  = "rg1"
    storage_account_name = "storagedev01"
    container_name       = "tfstate"
    key                  = "databricks-account-terraform.tfstate"
    subscription_id      = ""
    client_id            = ""
    tenant_id            = ""
  }
}

provider "azurerm" {
  features {}
}

provider "databricks" {
  host = var.databricks_host
}
