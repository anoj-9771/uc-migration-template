# # external locations for existing storage account
resource "databricks_external_location" "raw_ext" {
  name = "${var.env}raw_ext"
  url = format("abfss://%s@%s.dfs.core.windows.net",
    "raw",
  "${var.storage_account}")
  credential_name = databricks_storage_credential.main.name
  comment         = "Location pointing to raw container in ${var.storage_account}."
  depends_on      = [databricks_storage_credential.main]
  owner           = var.admin_group
}

resource "databricks_external_location" "cleansed_ext" {
  name = "${var.env}cleansed_ext"
  url = format("abfss://%s@%s.dfs.core.windows.net",
    "cleansed",
  "${var.storage_account}")
  credential_name = databricks_storage_credential.main.name
  comment         = "Location pointing to cleansed container in ${var.storage_account}."
  depends_on      = [databricks_storage_credential.main]
  owner           = var.admin_group
}

resource "databricks_external_location" "curated_ext" {
  name = "${var.env}curated_ext"
  url = format("abfss://%s@%s.dfs.core.windows.net",
    "curated-v2",
  "${var.storage_account}")
  credential_name = databricks_storage_credential.main.name
  comment         = "Location pointing to curated container in ${var.storage_account}."
  depends_on      = [databricks_storage_credential.main]
  owner           = var.admin_group
}


resource "databricks_external_location" "stage_ext" {
  name = "${var.env}stage_ext"
  url = format("abfss://%s@%s.dfs.core.windows.net",
    "stage",
  "${var.storage_account}")
  credential_name = databricks_storage_credential.main.name
  comment         = "Location pointing to stage container in ${var.storage_account}."
  depends_on      = [databricks_storage_credential.main]
  owner           = var.admin_group
}

resource "databricks_external_location" "landing_ext" {
  name = "${var.env}landing_ext"
  url = format("abfss://%s@%s.dfs.core.windows.net",
    "landing",
  "${var.storage_account}")
  credential_name = databricks_storage_credential.main.name
  comment         = "Location pointing to landing container in ${var.storage_account}."
  depends_on      = [databricks_storage_credential.main]
  owner           = var.admin_group
}

resource "databricks_external_location" "rejected_ext" {
  name = "${var.env}rejected_ext"
  url = format("abfss://%s@%s.dfs.core.windows.net",
    "rejected",
  "${var.storage_account}")
  credential_name = databricks_storage_credential.main.name
  comment         = "Location pointing to rejected container in ${var.storage_account}."
  depends_on      = [databricks_storage_credential.main]
  owner           = var.admin_group
}

# external locations for new storage account (using managed identity)
resource "databricks_external_location" "raw_mgd" {
  name = "${var.env}raw_mgd"
  url = format("abfss://%s@%s.dfs.core.windows.net/raw",
    "${var.container_name}",
  "${var.catalog_storage_account}")
  credential_name = databricks_storage_credential.main.name
  comment         = "Location pointing to raw folder in ${var.container_name} container in ${var.catalog_storage_account} for UC managed tables for UC managed tables."
  depends_on      = [databricks_storage_credential.main]
  owner           = var.admin_group
}

resource "databricks_external_location" "cleansed_mgd" {
  name = "${var.env}cleansed_mgd"
  url = format("abfss://%s@%s.dfs.core.windows.net/cleansed",
    "${var.container_name}",
  "${var.catalog_storage_account}")
  credential_name = databricks_storage_credential.main.name
  comment         = "Location pointing to cleansed folder in ${var.container_name} container in ${var.catalog_storage_account} for UC managed tables."
  depends_on      = [databricks_storage_credential.main]
  owner           = var.admin_group
}

resource "databricks_external_location" "curated_mgd" {
  name = "${var.env}curated_mgd"
  url = format("abfss://%s@%s.dfs.core.windows.net/curated",
    "${var.container_name}",
  "${var.catalog_storage_account}")
  credential_name = databricks_storage_credential.main.name
  comment         = "Location pointing to curated folder in ${var.container_name} container in ${var.catalog_storage_account} for UC managed tables."
  depends_on      = [databricks_storage_credential.main]
  owner           = var.admin_group
}

resource "databricks_external_location" "semantic_mgd" {
  name = "${var.env}semantic_mgd"
  url = format("abfss://%s@%s.dfs.core.windows.net/semantic",
    "${var.container_name}",
  "${var.catalog_storage_account}")
  credential_name = databricks_storage_credential.main.name
  comment         = "Location pointing to semantic folder in ${var.container_name} container in ${var.catalog_storage_account} for UC managed tables."
  depends_on      = [databricks_storage_credential.main]
  owner           = var.admin_group
}


# external location for uc_migration_logs
# possibly move these to ../databricks-account/module/external_location.tf
resource "databricks_external_location" "uc_migration_logs" {
  count = var.create_one_off ? 1 : 0
  # review the logging mechanism for the migration script
  name  = "uc_migration_logs"
  url = format("abfss://%s@%s.dfs.core.windows.net/uc_migration",
    "logging",
  "sadafprod03")
  credential_name = "795b0351-863e-4418-89eb-7fdde78b27bf"
  comment         = "Location pointing to uc_migration folder in ${var.container_name} container in ${var.catalog_storage_account} for UC managed tables."
  depends_on      = [databricks_storage_credential.main]
  owner           = var.admin_group
}

# external location for stage

resource "databricks_external_location" "stage_mgd" {
  name = "${var.env}stage_mgd"
  url = format("abfss://%s@%s.dfs.core.windows.net/stage",
    "${var.container_name}",
  "${var.catalog_storage_account}")
  credential_name = databricks_storage_credential.main.name
  comment         = "Location pointing to stage folder in ${var.container_name} container in ${var.catalog_storage_account} for UC managed tables."
  depends_on      = [databricks_storage_credential.main]
  owner           = var.admin_group
}

# external location for landing

resource "databricks_external_location" "rejected_mgd" {
  name = "${var.env}rejected_mgd"
  url = format("abfss://%s@%s.dfs.core.windows.net/rejected",
    "${var.container_name}",
  "${var.catalog_storage_account}")
  credential_name = databricks_storage_credential.main.name
  comment         = "Location pointing to rejected folder in ${var.container_name} container in ${var.catalog_storage_account} for UC managed tables."
  depends_on      = [databricks_storage_credential.main]
  owner           = var.admin_group
}
