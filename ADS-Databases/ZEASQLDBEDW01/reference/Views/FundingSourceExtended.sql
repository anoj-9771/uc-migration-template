

Create   View [reference].[FundingSourceExtended]
as

select fs.FundingSourceCode, FundingSourceName, FundingSourceDescription, Active 
    , af.AvetmissFundID, AvetmissFundName
	, fs.SpecificFundingCodeID, SpecificFundingName
	, fsvfh.VETFeeHelpFundID, vfh.VETFeeHelpFundName
	, fsc.CoreFundID
	, (case
		when CoreFundID = 1 then 'Core' 
		when CoreFundID = 2 then 'Non-Core'
		else NULL
	  end) AS CoreFund
	, bsc.BPRSubCategoryID, bsc.BPRSubCategoryName, bsc.BPRSubCategoryDescription
	, bc.BPRCategoryID, bc.BPRCategoryName
	, tf.TSNSWFundCode, tf.TSNSWFundName, tf.TSNSWFundDescription
	, tfg.TSNSWFundGroupCode, tfg.TSNSWFundGroupName, tfg.TSNSWFundGroupDescription
from reference.FundingSource fs
left join reference.AvetmissFund af on af.AvetmissFundID = fs.FundingSourceNationalID  and af._RecordDeleted = 0 and af._RecordCurrent = 1
left join reference.FundingSourceBpr fsb on fs.FundingSourceCode = fsb.FundingSourceCode  and fsb._RecordDeleted = 0 and fsb._RecordCurrent = 1
left join reference.BprSubCategory bsc on fsb.BPRSubCategoryID = bsc.BPRSubCategoryID  and bsc._RecordDeleted = 0 and bsc._RecordCurrent = 1
left join reference.BprCategory bc on bc.bprcategoryid = bsc.bprcategoryid  and bc._RecordDeleted = 0 and bc._RecordCurrent = 1
left join reference.SpecificFunding sf on sf.SpecificFundingCodeID = fs.SpecificFundingCodeID  and sf._RecordDeleted = 0 and sf._RecordCurrent = 1
left join reference.FundingSourceVetFeeHelpFund fsvfh on fsvfh.FundingSourceCode = fs.FundingSourceCode  and fsvfh._RecordDeleted = 0 and fsvfh._RecordCurrent = 1
left join reference.VetFeeHelpFund vfh on vfh.VETFeeHelpFundID = fsvfh.VETFeeHelpFundID  and vfh._RecordDeleted = 0 and vfh._RecordCurrent = 1
left join reference.FundingSourceCoreFund fsc on fsc.FundingSourceCode = fs.FundingSourceCode  and fsc._RecordDeleted = 0 and fsc._RecordCurrent = 1
left join reference.TsNswFund tf on tf.FundingSourceCode = fs.FundingSourceCode  and tf._RecordDeleted = 0 and tf._RecordCurrent = 1
left join reference.TsNswFundGroup tfg on tfg.FundingSourceNationalID = fs.FundingSourceNationalID  and tfg._RecordDeleted = 0 and tfg._RecordCurrent = 1
Where 	fs._RecordDeleted = 0 
and		fs._RecordCurrent = 1
--Can't map SBI as there are overriding business rules based on other enrolment factors. Mapping it here would provide incorrect data results.