resource "databricks_catalog" "raw" {
  metastore_id = var.metastore_id
  name         = "${var.env}raw"
  storage_root = databricks_external_location.raw_mgd.url
  owner        = var.admin_group
  comment = "Contains all managed raw schemas."
}

resource "databricks_catalog" "cleansed" {
  metastore_id = var.metastore_id
  name         = "${var.env}cleansed"
  storage_root = databricks_external_location.cleansed_mgd.url
  owner = var.admin_group
  comment = "Contains all managed cleansed schemas."
}


resource "databricks_catalog" "curated" {
  metastore_id = var.metastore_id
  name         = "${var.env}curated"
  storage_root = databricks_external_location.curated_mgd.url
  owner        = var.admin_group
  comment = "Contains all managed curated schemas."
}

resource "databricks_catalog" "semantic" {
  metastore_id = var.metastore_id
  name         = "${var.env}semantic"
  storage_root = databricks_external_location.semantic_mgd.url
  owner        = var.admin_group
  comment = "Contains all manageds semantic schemas."
}

resource "databricks_catalog" "stage" {
  metastore_id = var.metastore_id
  name         = "${var.env}stage"
  storage_root = databricks_external_location.stage_mgd.url
  owner        = var.admin_group
  comment = "Contains all managed stage schemas."
}

resource "databricks_catalog" "rejected" {
  metastore_id = var.metastore_id
  name         = "${var.env}rejected"
  storage_root = databricks_external_location.rejected_mgd.url
  owner        = var.admin_group
  comment = "Contains all managed rejected schemas."
}

resource "databricks_catalog" "uc_migration" {
  count = var.create_one_off ? 1:0
  metastore_id = var.metastore_id
  name         = "uc_migration"
  storage_root = databricks_external_location.uc_migration_logs[0].url
  owner        = var.admin_group
  comment = "Contains all Unity Catalog migration logs."
}

