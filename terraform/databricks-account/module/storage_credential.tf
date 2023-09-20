resource "databricks_storage_credential" "main" {
  name = var.managed_identity_name
  azure_managed_identity {
    access_connector_id = var.access_connector_id
    }
  comment = "Managed identity to connect to storage accounts: ${var.storage_account} & ${var.catalog_storage_account}."
  owner = var.admin_group
}