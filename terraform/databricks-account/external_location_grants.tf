# GRANTS for datalab external locations for files (not tables)
resource "databricks_grants" "datalab_swc_files" {
  external_location = "datalab_swc_files"
  grant {
    principal  = "A-Azure-rg-swcprod01-prod-daf-01-DataAdmin"
    privileges = ["READ_FILES","WRITE_FILES","CREATE_EXTERNAL_TABLE"]
  }
  grant {
    principal  = "A-Azure-rg-swcprod01-prod-daf-01-DataDeveloper"
    privileges = ["READ_FILES","WRITE_FILES","CREATE_EXTERNAL_TABLE"]
  }
  grant {
    principal  = "A-Azure-rg-swcprod01-prod-daf-01-DataAnalystAdvUsr"
    privileges = ["READ_FILES","WRITE_FILES","CREATE_EXTERNAL_TABLE"]
  }
  grant {
    principal  = "A-Azure-rg-swcprod01-prod-daf-01-BiDev"
    privileges = ["READ_FILES","WRITE_FILES","CREATE_EXTERNAL_TABLE"]
  }  
  grant {
    principal  = "A-Azure-rg-swcprod01-prod-daf-01-BiAdvUsr"
    privileges = ["READ_FILES","WRITE_FILES","CREATE_EXTERNAL_TABLE"]
  }      
}

resource "databricks_grants" "datalab_preprod_files" {
  external_location = "datalab_preprod_files"
  grant {
    principal  = "A-Azure-rg-swcprod01-preprod-daf-01-DataAdmin"
    privileges = ["READ_FILES","WRITE_FILES","CREATE_EXTERNAL_TABLE"]
  }
  grant {
    principal  = "A-Azure-rg-swcprod01-preprod-daf-01-DataDeveloper"
    privileges = ["READ_FILES","WRITE_FILES","CREATE_EXTERNAL_TABLE"]
  }
  grant {
    principal  = "A-Azure-rg-swcprod01-preprod-daf-01-DataAnalystAdvUsr"
    privileges = ["READ_FILES","WRITE_FILES","CREATE_EXTERNAL_TABLE"]
  }   
}

resource "databricks_grants" "datalab_test_files" {
  external_location = "datalab_test_files"
  grant {
    principal  = "A-Azure-rg-swcnonprod01-test-daf-01-DataAdmin"
    privileges = ["READ_FILES","WRITE_FILES","CREATE_EXTERNAL_TABLE"]
  }
  grant {
    principal  = "A-Azure-rg-swcnonprod01-test-daf-01-DataDeveloper"
    privileges = ["READ_FILES","WRITE_FILES","CREATE_EXTERNAL_TABLE"]
  }
  grant {
    principal  = "A-Azure-rg-swcnonprod01-test-daf-01-DataAnalystAdvUsr"
    privileges = ["READ_FILES","WRITE_FILES","CREATE_EXTERNAL_TABLE"]
  }   
}

resource "databricks_grants" "datalab_dev_files" {
  external_location = "datalab_dev_files"
  grant {
    principal  = "A-Azure-rg-swcnonprod01-dev-daf-01-DataAdmin"
    privileges = ["READ_FILES","WRITE_FILES","CREATE_EXTERNAL_TABLE"]
  }
  grant {
    principal  = "A-Azure-rg-swcnonprod01-dev-daf-01-DataDeveloper"
    privileges = ["READ_FILES","WRITE_FILES","CREATE_EXTERNAL_TABLE"]
  }
  grant {
    principal  = "A-Azure-rg-swcnonprod01-dev-daf-01-DataAnalystAdvUsr"
    privileges = ["READ_FILES","WRITE_FILES","CREATE_EXTERNAL_TABLE"]
  }   
  grant {
    principal  = "A-Azure-rg-swcnonprod01-dev-daf-01-BiDeveloper"
    privileges = ["READ_FILES","WRITE_FILES","CREATE_EXTERNAL_TABLE"]
  }   
  grant {
    principal  = "A-Azure-rg-swcnonprod01-dev-daf-01-BiAdvUsr"
    privileges = ["READ_FILES","WRITE_FILES","CREATE_EXTERNAL_TABLE"]
  }       
}
