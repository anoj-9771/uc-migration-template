###################################################################
#GRANTS for datalab schemas
resource "databricks_grants" "datalab_swc" {
  schema = databricks_schema.datalab_swc.id
  grant {
    principal  = "A-Azure-rg-swcprod01-prod-daf-01-DataAdmin"
    privileges = ["ALL_PRIVILEGES"]
  }
  grant {
    principal  = "A-Azure-rg-swcprod01-prod-daf-01-DataDeveloper"
    privileges = ["ALL_PRIVILEGES"]
  }
  grant {
    principal  = "A-Azure-rg-swcprod01-prod-daf-01-DataAnalystAdvUsr"
    privileges = ["ALL_PRIVILEGES"]
  }
  grant {
    principal  = "A-Azure-rg-swcprod01-prod-daf-01-BiDev"
    privileges = ["ALL_PRIVILEGES"]
  }  
  grant {
    principal  = "A-Azure-rg-swcprod01-prod-daf-01-BiAdvUsr"
    privileges = ["ALL_PRIVILEGES"]
  }      
}

resource "databricks_grants" "datalab_preprod" {
  schema = databricks_schema.datalab_preprod.id
  grant {
    principal  = "A-Azure-rg-swcprod01-preprod-daf-01-DataAdmin"
    privileges = ["ALL_PRIVILEGES"]
  }
  grant {
    principal  = "A-Azure-rg-swcprod01-preprod-daf-01-DataDeveloper"
    privileges = ["ALL_PRIVILEGES"]
  }
  grant {
    principal  = "A-Azure-rg-swcprod01-preprod-daf-01-DataAnalystAdvUsr"
    privileges = ["ALL_PRIVILEGES"]
  }   
}

resource "databricks_grants" "datalab_test" {
  schema = databricks_schema.datalab_test.id
  grant {
    principal  = "A-Azure-rg-swcnonprod01-test-daf-01-DataAdmin"
    privileges = ["ALL_PRIVILEGES"]
  }
  grant {
    principal  = "A-Azure-rg-swcnonprod01-test-daf-01-DataDeveloper"
    privileges = ["ALL_PRIVILEGES"]
  }
  grant {
    principal  = "A-Azure-rg-swcnonprod01-test-daf-01-DataAnalystAdvUsr"
    privileges = ["ALL_PRIVILEGES"]
  }   
}

resource "databricks_grants" "datalab_dev" {
  schema = databricks_schema.datalab_dev.id
  grant {
    principal  = "A-Azure-rg-swcnonprod01-dev-daf-01-DataAdmin"
    privileges = ["ALL_PRIVILEGES"]
  }
  grant {
    principal  = "A-Azure-rg-swcnonprod01-dev-daf-01-DataDeveloper"
    privileges = ["ALL_PRIVILEGES"]
  }
  grant {
    principal  = "A-Azure-rg-swcnonprod01-dev-daf-01-DataAnalystAdvUsr"
    privileges = ["ALL_PRIVILEGES"]
  }   
  grant {
    principal  = "A-Azure-rg-swcnonprod01-dev-daf-01-BiDeveloper"
    privileges = ["ALL_PRIVILEGES"]
  }   
  grant {
    principal  = "A-Azure-rg-swcnonprod01-dev-daf-01-BiAdvUsr"
    privileges = ["ALL_PRIVILEGES"]
  }        
}