
CREATE view  reference.CommencingProgramIdentifierescnm
as select [ESCNM], [ESCNM_Description], [_DLRawZoneTimeStamp], [_DLTrustedZoneTimeStamp], [_RecordStart], [_RecordEnd], [_RecordDeleted], [_RecordCurrent]
from [edw].[reference_CommencingProgramIdentifierescnm]