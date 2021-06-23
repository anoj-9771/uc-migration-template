

--exec compliance.PopulateAvetmissFeeRecord

create procedure compliance.PopulateAvetmissFeeRecord
as

	if object_id('tempdb..#feeList') is not null drop table #feeList

	select
		 fl.fee_record_code
		,fl.fes_record_type
		,fl.status
		,fl.matched_fee_record_code
		,fl.fes_associated_record
	into #feeList
	from edw.oneebs_ebs_0165_fees_list fl
	where _RecordCurrent = 1
	and _RecordDeleted = 0

	if object_id('tempdb..#seed') is not null drop table #seed
	select 
		fl.FEE_RECORD_CODE as start_fee_record_code,
		f2.fee_record_code as fee_record_code,
		case when f2.status = 'f' then f2.fee_record_code else 0 end as weight,
		f2.fes_associated_record
	into #seed
	from #feeList fl
		join #feeList f2 on f2.fee_record_code = CASE WHEN fl.fes_record_type = 'R' THEN fl.matched_fee_record_code ELSE fl.fee_record_code END

	create index IDX_seed_fee_record_code on #seed(fee_record_code)

	--------------------------

	--firstFeeRecord
	if object_id('tempdb..#firstFeeRecord') is not null drop table #firstFeeRecord
	;with cte_next as
	(
		select seed.start_fee_record_code, seed.fee_record_code, seed.fes_associated_record
		from #seed seed
		union all
		select t.start_fee_record_code, seed.fee_record_code, seed.fes_associated_record
		from cte_next t
			join #seed seed on t.fes_associated_record = seed.fee_record_code
	)
	select start_fee_record_code as fee_record_code, min(fee_record_code) as firstFeeRecord
	into #firstFeeRecord
	from cte_next
	group by start_fee_record_code
	option (maxrecursion 0)

	--------------------------
	--finalFeeRecord
	if object_id('tempdb..#finalFeeRecord') is not null drop table #finalFeeRecord
	;with cte_next as
	(
		select seed.start_fee_record_code, seed.fee_record_code, seed.weight, seed.fes_associated_record
		from #seed seed
		union all
		select t.start_fee_record_code, seed.fee_record_code, seed.weight, seed.fes_associated_record
		from cte_next t
			join #seed seed on t.fee_record_code = seed.fes_associated_record
	) 
	select start_fee_record_code as fee_record_code, case when max(weight) > 0 then max(weight) else max(fee_record_code) end as finalFeeRecord
	into #finalFeeRecord
	from cte_next
	group by start_fee_record_code
	option (maxrecursion 0)

	-- combine #firstFeeRecord and #finalFeeRecord and insert into the physical table
	truncate table compliance.AvetmissFeeRecord
	insert into compliance.AvetmissFeeRecord (FeeRecordCode, FirstFeeRecord, FinalFeeRecord)
	select f1.fee_record_code, f1.firstFeeRecord, f2.finalFeeRecord
	from #firstFeeRecord f1
		join #finalFeeRecord f2 on f1.fee_record_code = f2.fee_record_code

	if object_id('tempdb..#feeList') is not null drop table #feeList
	if object_id('tempdb..#seed') is not null drop table #seed
	if object_id('tempdb..#firstFeeRecord') is not null drop table #firstFeeRecord
	if object_id('tempdb..#finalFeeRecord') is not null drop table #finalFeeRecord