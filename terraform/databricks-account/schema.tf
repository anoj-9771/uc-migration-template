# datalab schemas
resource "databricks_schema" "datalab_dev" {
  owner        = var.admin_group
  catalog_name = databricks_catalog.datalab.id
  name         = "dev"
  storage_root = databricks_external_location.datalab_dev_mgd.url
  comment      = "Schema for tables migrated from datalab schema in dev hive mestastore. This resource is managed by Terraform."
}

resource "databricks_schema" "datalab_test" {
  owner        = var.admin_group
  catalog_name = databricks_catalog.datalab.id
  name         = "test"
  storage_root = databricks_external_location.datalab_test_mgd.url
  comment      = "Schema for tables migrated from datalab schema in test hive mestastore. This resource is managed by Terraform."
}


resource "databricks_schema" "datalab_preprod" {
  owner        = var.admin_group
  catalog_name = databricks_catalog.datalab.id
  name         = "preprod"
  storage_root = databricks_external_location.datalab_preprod_mgd.url
  comment      = "Schema for tables migrated from datalab schema in preprod hive mestastore. This resource is managed by Terraform."
}


resource "databricks_schema" "datalab_swc" {
  owner        = var.admin_group
  catalog_name = databricks_catalog.datalab.id
  name         = "swc"
  storage_root = databricks_external_location.datalab_swc_mgd.url
  comment      = "Schema for tables migrated from datalab schema in prod hive mestastore. This resource is managed by Terraform."
}
