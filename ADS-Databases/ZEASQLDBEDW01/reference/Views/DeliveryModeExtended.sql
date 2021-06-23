

CREATE   View [reference].[DeliveryModeExtended]
as

Select dm.DeliveryModeCode
, DeliveryModeName
, DeliveryModeDescription
, dm.Active
, admp.AvetmissDeliveryModeID
, admp.AvetmissDeliveryModeDescription as AvetmissDeliveryModeDescriptionPre2018
, adm.AvetmissDeliveryModeCode
, Internal
, [External]
, WorkplaceBased
, adm.AvetmissDeliveryModeDescription
from reference.DeliveryMode dm
left join reference.DeliveryModeAvetmissDeliveryMode dmadm on dmadm.DeliveryModeCode = dm.DeliveryModeCode and dmadm._RecordDeleted = 0 and dmadm._RecordCurrent = 1
left join reference.AvetmissDeliveryModePre2018 admp on admp.AvetmissDeliveryModeID = dm.AvetmissDeliveryModeId and admp._RecordDeleted = 0 and admp._RecordCurrent = 1
left join reference.AvetmissDeliveryMode adm on adm.AvetmissDeliveryModeCode = dmadm.AvetmissDeliveryModeCode and adm._RecordDeleted = 0 and adm._RecordCurrent = 1
where dm._RecordDeleted = 0
and dm._RecordCurrent = 1