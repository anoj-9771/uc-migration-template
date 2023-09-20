variable "databricks_host" {}

terraform {
  required_providers {
    azurerm = {
      source = "hashicorp/azurerm"
    }
    databricks = {
      source = "databricks/databricks"
    }
  }
  backend "azurerm" {
    resource_group_name  = "rg-swcnonprod01-daf-dev-01"
    storage_account_name = "sadafdev01"
    container_name       = "tfstate"
    key                  = "dev-terraform.tfstate"
    subscription_id      = "e59a4313-66c3-4db6-842d-6154c5e08205"
    client_id            = "acff96db-8630-433b-bbb1-35a3813fa036"
    tenant_id            = "8351bb5c-749d-4ee4-b1c4-71a3971acbe9"
  }
}

provider "azurerm" {
  features {}
  # subscription_id = "e59a4313-66c3-4db6-842d-6154c5e08205"
  # client_id       = "acff96db-8630-433b-bbb1-35a3813fa036"
  # tenant_id       = "8351bb5c-749d-4ee4-b1c4-71a3971acbe9"
}

# Use Azure CLI authentication.
provider "databricks" {
  host = var.databricks_host
}

