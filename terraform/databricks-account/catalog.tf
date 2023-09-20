
resource "databricks_catalog" "datalab" {
  name         = "datalab"
  owner        = var.admin_group
  comment = "Contains all managed datalab schemas."
}


variable "metastore_id" {default = "4af25536-9b46-43ee-bae7-7be48318808c"}
variable "admin_group" {default = "swc-metastore-admin"}
variable "databricks_host" {default = "https://adb-2536357900957616.16.azuredatabricks.net/"}
