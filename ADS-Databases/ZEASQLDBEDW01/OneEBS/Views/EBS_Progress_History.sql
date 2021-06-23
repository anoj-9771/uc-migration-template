

CREATE VIEW [OneEBS].[EBS_Progress_History] AS  
SELECT 
	  pr.people_units_id parent_id,
	  pr.rpr_sequence id,
	  pr.people_units_id,
	  pr.people_units_type,
	  pr.effective_date,
	  pr.actual_end_date,
	  pr.reg_registration_code,
	  pr.rpc_progress_type progress_status,
	  pr.rpr_sequence,
	  pr.rpc_type_name progress_code,
	  pr.reg_registration_number,
	  pr.created_by,
	  pr.created_date,
	  pr.updated_by,
	  pr.updated_date,
	  pr.fes_registration_number,
	  pr.fes_unit_instance_code,
	  pr.app_application_number,
	  pr.au_au_number,
	  pr.per_person_code,
	  pr.fes_cal_occurrence,
	  pr.rul_code,
	  pr.actioned_by,
	  pr.actioned_date,
	  pr.action_status,
	  pr.submitted_by,
	  pr.notes_id,
	  pr.batch_no,
	  pr.user_notified,
	  COALESCE(pc.fes_long_description, pc.fes_short_description) [description],
	  pc.student_status,
	  COALESCE(v.fes_long_description, v.fes_short_description) unit_type,
	  pu.person_code,
	  COALESCE(uio.long_description, ui.fes_long_description) course_description,
	  pu.progress_status unit_status,

	  --To_Char(pr.effective_date, 'DD/MM/YYYY') friendly_effective_date,
	  convert(date, pr.effective_date) as friendly_effective_date,
	  --To_Char(pr.created_date, 'DD/MM/YYYY') friendly_created_date,
	   convert(date, pr.created_date) as friendly_created_date,

	  concat(pr.fes_unit_instance_code , ' - ' , pr.fes_cal_occurrence , ' - ' , pr.effective_date) as  unique_order,
	  CASE pr.people_units_type
			WHEN 'E' THEN 'Enquiry'
			WHEN 'A' THEN 'Application'
			WHEN 'R' THEN 'Enrolment'
			ELSE NULL
	  END AS people_unit_type,
	  pr.cascade_aims,
	  pr.cascade_exams,
	  pr.cascade_registers,
	  pr.cascade_units,
	  pr.destination

FROM [OneEBS].[Progress_Records] pr
	INNER JOIN [OneEBS].[Progress_Codes] pc 
		ON pc.type_name = pr.rpc_type_name
	INNER JOIN [OneEBS].[Verifiers] v 
		ON pc.student_status = v.low_value 
		AND  v.rv_domain = 'PEOPLE_UNITS_TYPE'
	INNER JOIN [OneEBS].[People_Units] pu 
		ON pu.id = pr.people_units_id
	LEFT OUTER JOIN [OneEBS].[Unit_Instance_Occurrences] uio 
		ON uio.uio_id = pu.uio_id
	LEFT OUTER JOIN [OneEBS].[Unit_Instances] ui 
		ON uio.fes_uins_instance_code = ui.fes_unit_instance_code

WHERE isnull(pr.action_status,3) = 3 -- show only accepted progressions