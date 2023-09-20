# datalab external locations
resource "databricks_external_location" "datalab_dev_mgd" {
  name = "datalab_dev_mgd"
  url = format("abfss://%s@%s.dfs.core.windows.net/dev/tables",
    "datalab",
  "sadafprod03")
  credential_name = "795b0351-863e-4418-89eb-7fdde78b27bf"
  comment         = "Location pointing to dev/tables folder in datalab container in sadafprod03."
  owner           = var.admin_group
}

resource "databricks_external_location" "datalab_dev_files" {
  name = "datalab_dev_files"
  url = format("abfss://%s@%s.dfs.core.windows.net/dev/files",
    "datalab",
  "sadafprod03")
  credential_name = "795b0351-863e-4418-89eb-7fdde78b27bf"
  comment         = "Location pointing to dev/files folder in datalab container in sadafprod03."
  owner           = var.admin_group
}

resource "databricks_external_location" "datalab_test_mgd" {
  name = "datalab_test_mgd"
  url = format("abfss://%s@%s.dfs.core.windows.net/test/tables",
    "datalab",
  "sadafprod03")
  credential_name = "795b0351-863e-4418-89eb-7fdde78b27bf"
  comment         = "Location pointing to test/tables folder in datalab container in sadafprod03."
  owner           = var.admin_group
}

resource "databricks_external_location" "datalab_test_files" {
  name = "datalab_test_files"
  url = format("abfss://%s@%s.dfs.core.windows.net/test/files",
    "datalab",
  "sadafprod03")
  credential_name = "795b0351-863e-4418-89eb-7fdde78b27bf"
  comment         = "Location pointing to test/files folder in datalab container in sadafprod03."
  owner           = var.admin_group
}

resource "databricks_external_location" "datalab_preprod_mgd" {
  name = "datalab_preprod_mgd"
  url = format("abfss://%s@%s.dfs.core.windows.net/preprod/tables",
    "datalab",
  "sadafprod03")
  credential_name = "795b0351-863e-4418-89eb-7fdde78b27bf"
  comment         = "Location pointing to preprod/tables folder in datalab container in sadafprod03."
  owner           = var.admin_group
}

resource "databricks_external_location" "datalab_preprod_files" {
  name = "datalab_preprod_files"
  url = format("abfss://%s@%s.dfs.core.windows.net/preprod/files",
    "datalab",
  "sadafprod03")
  credential_name = "795b0351-863e-4418-89eb-7fdde78b27bf"
  comment         = "Location pointing to preprod/files folder in datalab container in sadafprod03."
  owner           = var.admin_group
}

resource "databricks_external_location" "datalab_swc_mgd" {
  name = "datalab_swc_mgd"
  url = format("abfss://%s@%s.dfs.core.windows.net/swc/tables",
    "datalab",
  "sadafprod03")
  credential_name = "795b0351-863e-4418-89eb-7fdde78b27bf"
  comment         = "Location pointing to swc/tables folder in datalab container in sadafprod03."
  owner           = var.admin_group
}

resource "databricks_external_location" "datalab_swc_files" {
  name = "datalab_swc_files"
  url = format("abfss://%s@%s.dfs.core.windows.net/swc/files",
    "datalab",
  "sadafprod03")
  credential_name = "795b0351-863e-4418-89eb-7fdde78b27bf"
  comment         = "Location pointing to swc/files folder in datalab container in sadafprod03."
  owner           = var.admin_group
}
