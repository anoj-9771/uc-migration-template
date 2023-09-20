resource "databricks_grants" "raw_ext" {
  external_location = databricks_external_location.raw_ext.id
  grant {
    principal  = "${var.db_service_principal}"
    privileges = ["READ_FILES","WRITE_FILES","CREATE_EXTERNAL_TABLE"]
  }
  grant {
    principal  = "${var.adf_service_principal}"
    privileges = ["READ_FILES","WRITE_FILES","CREATE_EXTERNAL_TABLE"]
  }  
  grant {
    principal  = "A-Azure-${var.resource_group}-DataAdmin"
    privileges = ["READ_FILES","WRITE_FILES","CREATE_EXTERNAL_TABLE"]
  }
  dynamic "grant" {
    for_each = var.data_developer_grant[*]
    content {
      principal  = "A-Azure-${var.resource_group}-DataDeveloper"
      privileges = ["READ_FILES","WRITE_FILES","CREATE_EXTERNAL_TABLE"]
    }
  } 
}

resource "databricks_grants" "cleansed_ext" {
  external_location = databricks_external_location.cleansed_ext.id
  grant {
    principal  = "${var.db_service_principal}"
    privileges = ["READ_FILES","WRITE_FILES","CREATE_EXTERNAL_TABLE"]
  }
  grant {
    principal  = "${var.adf_service_principal}"
    privileges = ["READ_FILES","WRITE_FILES","CREATE_EXTERNAL_TABLE"]
  }    
  grant {
    principal  = "A-Azure-${var.resource_group}-DataAdmin"
    privileges = ["READ_FILES","WRITE_FILES","CREATE_EXTERNAL_TABLE"]
  }
  dynamic "grant" {
    for_each = var.data_developer_grant[*]
    content {
      principal  = "A-Azure-${var.resource_group}-DataDeveloper"
      privileges = ["READ_FILES","WRITE_FILES","CREATE_EXTERNAL_TABLE"]
    }
  }    
}

resource "databricks_grants" "curated_ext" {
  external_location = databricks_external_location.curated_ext.id
  grant {
    principal  = "${var.db_service_principal}"
    privileges = ["READ_FILES","WRITE_FILES","CREATE_EXTERNAL_TABLE"]
  }
  grant {
    principal  = "${var.adf_service_principal}"
    privileges = ["READ_FILES","WRITE_FILES","CREATE_EXTERNAL_TABLE"]
  }    
  grant {
    principal  = "A-Azure-${var.resource_group}-DataAdmin"
    privileges = ["READ_FILES","WRITE_FILES","CREATE_EXTERNAL_TABLE"]
  }
  dynamic "grant" {
    for_each = var.data_developer_grant[*]
    content {
      principal  = "A-Azure-${var.resource_group}-DataDeveloper"
      privileges = ["READ_FILES","WRITE_FILES","CREATE_EXTERNAL_TABLE"]
    }
  }   
}

resource "databricks_grants" "stage_ext" {
  external_location = databricks_external_location.stage_ext.id
  grant {
    principal  = "${var.db_service_principal}"
    privileges = ["READ_FILES","WRITE_FILES","CREATE_EXTERNAL_TABLE"]
  }
  grant {
    principal  = "${var.adf_service_principal}"
    privileges = ["READ_FILES","WRITE_FILES","CREATE_EXTERNAL_TABLE"]
  }    
  grant {
    principal  = "A-Azure-${var.resource_group}-DataAdmin"
    privileges = ["READ_FILES","WRITE_FILES","CREATE_EXTERNAL_TABLE"]
  }
  dynamic "grant" {
    for_each = var.data_developer_grant[*]
    content {
      principal  = "A-Azure-${var.resource_group}-DataDeveloper"
      privileges = ["READ_FILES","WRITE_FILES","CREATE_EXTERNAL_TABLE"]
    }
  }   
}

resource "databricks_grants" "landing_ext" {
  external_location = databricks_external_location.landing_ext.id
  grant {
    principal  = "${var.db_service_principal}"
    privileges = ["READ_FILES","WRITE_FILES","CREATE_EXTERNAL_TABLE"]
  }
  grant {
    principal  = "${var.adf_service_principal}"
    privileges = ["READ_FILES","WRITE_FILES","CREATE_EXTERNAL_TABLE"]
  }    
  grant {
    principal  = "A-Azure-${var.resource_group}-DataAdmin"
    privileges = ["READ_FILES","WRITE_FILES","CREATE_EXTERNAL_TABLE"]
  }
  dynamic "grant" {
    for_each = var.data_developer_grant[*]
    content {
      principal  = "A-Azure-${var.resource_group}-DataDeveloper"
      privileges = ["READ_FILES","WRITE_FILES","CREATE_EXTERNAL_TABLE"]
    }
  }   
}

resource "databricks_grants" "rejected_ext" {
  external_location = databricks_external_location.rejected_ext.id
  grant {
    principal  = "${var.db_service_principal}"
    privileges = ["READ_FILES","WRITE_FILES","CREATE_EXTERNAL_TABLE"]
  }
  grant {
    principal  = "${var.adf_service_principal}"
    privileges = ["READ_FILES","WRITE_FILES","CREATE_EXTERNAL_TABLE"]
  }    
  grant {
    principal  = "A-Azure-${var.resource_group}-DataAdmin"
    privileges = ["READ_FILES","WRITE_FILES","CREATE_EXTERNAL_TABLE"]
  }
  dynamic "grant" {
    for_each = var.data_developer_grant[*]
    content {
      principal  = "A-Azure-${var.resource_group}-DataDeveloper"
      privileges = ["READ_FILES","WRITE_FILES","CREATE_EXTERNAL_TABLE"]
    }
  }   
}