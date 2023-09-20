resource "databricks_grants" "raw"{
    catalog = databricks_catalog.raw.id
    dynamic "grant" {
        for_each = var.digital_ad_groups
        content{
            principal = grant.value
            privileges = ["USE_CATALOG","USE_SCHEMA","CREATE_SCHEMA"]
        }
    }
    dynamic "grant" {
        for_each = var.service_principals
        content{
            principal = grant.value
            privileges = ["ALL_PRIVILEGES"]
        }
    }    
}

resource "databricks_grants" "cleansed"{
    catalog = databricks_catalog.cleansed.id
    dynamic "grant" {
        for_each = var.digital_ad_groups
        content{
            principal = grant.value
            privileges = ["USE_CATALOG","USE_SCHEMA","CREATE_SCHEMA"]
        }
    }
    dynamic "grant" {
        for_each = var.service_principals
        content{
            principal = grant.value
            privileges = ["ALL_PRIVILEGES"]
        }
    } 
    grant {
        principal  = "A-Azure-${var.resource_group}-DataAnalystAdvUsr"
        privileges = ["USE_CATALOG"]
    }
    grant {
        principal  = "A-Azure-${var.resource_group}-DataAnalystStdUsr"
        privileges = ["USE_CATALOG"]
    }    
}

resource "databricks_grants" "curated"{
    catalog = databricks_catalog.curated.id
    dynamic "grant" {
        for_each = var.digital_ad_groups
        content{
            principal = grant.value
            privileges = ["USE_CATALOG","USE_SCHEMA","CREATE_SCHEMA"]
        }
    }
    dynamic "grant" {
        for_each = var.service_principals
        content{
            principal = grant.value
            privileges = ["ALL_PRIVILEGES"]
        }
    }  
    grant {
        principal  = "A-Azure-${var.resource_group}-DataAnalystAdvUsr"
        privileges = ["USE_CATALOG"]
    }
    grant {
        principal  = "A-Azure-${var.resource_group}-DataAnalystStdUsr"
        privileges = ["USE_CATALOG"]
    }        
}

resource "databricks_grants" "semantic"{
    catalog = databricks_catalog.semantic.id
    dynamic "grant" {
        for_each = var.digital_ad_groups
        content{
            principal = grant.value
            privileges = ["USE_CATALOG","CREATE_SCHEMA"]
        }
    }
    dynamic "grant" {
        for_each = var.service_principals
        content{
            principal = grant.value
            privileges = ["ALL_PRIVILEGES"]
        }
    }  
    grant {
        principal  = "A-Azure-${var.resource_group}-BiAdvUsr"
        privileges = ["USE_CATALOG"]
    }
    grant {
        principal  = "A-Azure-${var.resource_group}-BiStdUsr"
        privileges = ["USE_CATALOG"]
    }        
}
