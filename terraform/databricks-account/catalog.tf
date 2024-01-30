
resource "databricks_catalog" "datalab" {
  name         = "datalab"
  owner        = var.admin_group
  comment = "Contains all managed datalab schemas."
}


variable "metastore_id" {default = ""}
variable "admin_group" {default = "companyx-metastore-admin"}
variable "databricks_host" {default = ""}
