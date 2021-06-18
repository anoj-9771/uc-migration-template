Create View Reference.[BPRExtended]
as
select 
fs.FundingSourceCode
, FundingSourceName
, FundingSourceDescription
, Active
, bsc.BPRSubCategoryID
, bsc.BPRSubCategoryName
, bsc.BPRSubCategoryDescription
, bc.bprcategoryID
, bc.bprcategoryname
from reference.BprSubCategory bsc
left join reference.BprCategory bc on bc.bprcategoryid = bsc.bprcategoryid and bc._RecordDeleted = 0 and bc._RecordCurrent = 1
left join reference.FundingSourceBpr fsb on fsb.BPRSubCategoryID = bsc.BPRSubCategoryID and fsb._RecordDeleted = 0 and fsb._RecordCurrent = 1
left join reference.FundingSource fs on fs.FundingSourceCode = fsb.FundingSourceCode and fs._RecordDeleted = 0 and fs._RecordCurrent = 1

where bsc._RecordDeleted = 0
and bsc._RecordCurrent = 1