resource "databricks_cluster" "uc-migration-anoj" {
  cluster_name            = "uc-migration-anoj"
  node_type_id            = "Standard_DS5_v2"
  driver_node_type_id     = "Standard_DS5_v2"
  spark_version           = "12.2.x-scala2.12"
  autotermination_minutes = 20
  data_security_mode      = "SINGLE_USER"
  single_user_name        = "name@companyx.com.au"
  autoscale {
    min_workers = 2
    max_workers = 10
  }
  is_pinned = true
  library {
    pypi {
      package = "openpyxl==3.1.2"
    }
  }
  spark_conf = {
    "spark.databricks.delta.preview.enabled"           = "true"
    "spark.databricks.libraries.enableMavenResolution" = "false"
  }
}

resource "databricks_cluster" "uc-migration-kumar" {
  cluster_name            = "uc-migration-kumar"
  node_type_id            = "Standard_DS5_v2"
  driver_node_type_id     = "Standard_DS5_v2"
  spark_version           = "12.2.x-scala2.12"
  autotermination_minutes = 20
  data_security_mode      = "SINGLE_USER"
  single_user_name        = "o3hz@sydneywater.com.au"
  autoscale {
    min_workers = 2
    max_workers = 10
  }
  is_pinned = true
  library {
    pypi {
      package = "openpyxl==3.1.2"
    }
  }
  spark_conf = {
    "spark.databricks.delta.preview.enabled"           = "true"
    "spark.databricks.libraries.enableMavenResolution" = "false"
  }
}