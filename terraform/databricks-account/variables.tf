variable "digital_ad_groups" {
  type    = list(string)
  default = ["A-Azure-rg-swcprod01-prod-daf-01-DataAdmin","A-Azure-rg-swcprod01-prod-daf-01-DataDeveloper",
    "A-Azure-rg-swcprod01-preprod-daf-01-DataAdmin","A-Azure-rg-swcprod01-preprod-daf-01-DataDeveloper",
    "A-Azure-rg-swcnonprod01-test-daf-01-DataAdmin","A-Azure-rg-swcnonprod01-test-daf-01-DataDeveloper",
    "A-Azure-rg-swcnonprod01-dev-daf-01-DataAdmin","A-Azure-rg-swcnonprod01-dev-daf-01-DataDeveloper"]
}
variable "all_service_principals" {
  type    = list(string)
  default = ["407e7ff2-525f-4d47-90f9-1c22fbfd977f","d47f1828-3216-400f-85f8-49320a296492",
    "e3d4b951-6cbb-4ab4-aaf6-0c5ee7dc5a25","1af18bc3-a05d-423a-8f85-0d5ff002d19d",
    "fc2d958a-56c9-4855-a7ce-ad4407ff94ba","acff96db-8630-433b-bbb1-35a3813fa036",
    "13daf0ce-c685-41b1-b609-0747c83bc4ac","3012901b-9b8c-4100-a6bf-a1a2ec010def",
    "07ffd3b5-2923-4df9-aa50-8525c7a36bad"]
}