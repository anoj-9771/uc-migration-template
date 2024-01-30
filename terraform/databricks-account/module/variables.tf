variable "env" {}
variable "container_name" {}
variable "catalog_storage_account" {}
variable "storage_account" {}
variable "admin_group" {default = "companyx-metastore-admin"} 
variable "metastore_id" {default = ""}
variable "managed_identity_name" {}
variable "access_connector_id" {}
variable "resource_group" {}
variable "db_service_principal" {}
variable "adf_service_principal" {}
variable "digital_ad_groups" {
  type    = list(string)
  default = null
}
variable "service_principals" {
  type    = list(string)
  default = null
}
variable "data_developer_grant" {
    type = object({ included = string }) 
    default = null
    }
variable "create_one_off" {default = false}
variable "databricks_host" {default = ""}
