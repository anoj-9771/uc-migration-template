###################################################################
#datalab catalog grants
resource "databricks_grants" "datalab" {
  catalog = "datalab"
  dynamic "grant" {
    for_each = var.digital_ad_groups
    content {
      principal = grant.value
      privileges = ["USE_CATALOG","USE_SCHEMA","CREATE_SCHEMA"]
    }
  }
  dynamic "grant" {
    for_each = var.all_service_principals
    content {
      principal = grant.value
      privileges = ["USE_CATALOG","USE_SCHEMA","CREATE_SCHEMA"]
    }
  }  
  grant {
    principal  = "A-Azure-rg-swcprod01-prod-daf-01-DataAnalystAdvUsr"
    privileges = ["USE_CATALOG"]
  }
  grant {
    principal  = "A-Azure-rg-swcprod01-prod-daf-01-BiDev"
    privileges = ["USE_CATALOG"]
  }  
  grant {
    principal  = "A-Azure-rg-swcprod01-prod-daf-01-BiAdvUsr"
    privileges = ["USE_CATALOG"]
  }  
  grant {
    principal  = "A-Azure-rg-swcprod01-preprod-daf-01-DataAnalystAdvUsr"
    privileges = ["USE_CATALOG"]
  } 
  grant {
    principal  = "A-Azure-rg-swcnonprod01-test-daf-01-DataAnalystAdvUsr"
    privileges = ["USE_CATALOG"]
  }  
  grant {
    principal  = "A-Azure-rg-swcnonprod01-dev-daf-01-DataAnalystAdvUsr"
    privileges = ["USE_CATALOG"]
  }   
  grant {
    principal  = "A-Azure-rg-swcnonprod01-dev-daf-01-BiDeveloper"
    privileges = ["USE_CATALOG"]
  }   
  grant {
    principal  = "A-Azure-rg-swcnonprod01-dev-daf-01-BiAdvUsr"
    privileges = ["USE_CATALOG"]
  }           
}
###################################################################
# stage catalog grants
resource "databricks_grants" "stage" {
  catalog = "stage"
  dynamic "grant" {
    for_each = var.digital_ad_groups
    content {
      principal = grant.value
      privileges = ["ALL_PRIVILEGES"]
    }
  }
  dynamic "grant" {
    for_each = var.all_service_principals
    content {
      principal = grant.value
      privileges = ["ALL_PRIVILEGES"]
    }
  }           
}
###################################################################
# rejected catalog grants
resource "databricks_grants" "rejected" {
  catalog = "rejected"
  dynamic "grant" {
    for_each = var.digital_ad_groups
    content {
      principal = grant.value
      privileges = ["ALL_PRIVILEGES"]
    }
  }
  dynamic "grant" {
    for_each = var.all_service_principals
    content {
      principal = grant.value
      privileges = ["ALL_PRIVILEGES"]
    }
  }          
}