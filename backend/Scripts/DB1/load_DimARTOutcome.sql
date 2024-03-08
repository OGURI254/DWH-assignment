IF OBJECT_ID(N'[NDWH].[dbo].[DimARTOutcome]', N'U') IS NOT NULL 
	DROP TABLE [NDWH].[dbo].[DimARTOutcome];
BEGIN
	---DimARTOutcome
	with distinct_ARTOutcomes as (
		select 'S' as ARTOutcome
			union all
		select 'D' as ARTOutcome
			union all
		select 'L' as ARTOutcome
			union all
		select 'NV' as ARTOutcome
			union all
		select 'T' as ARTOutcome
			union all
		select 'V' as ARTOutcome
			union all
		select 'NP' as ARTOutcome
			union all
		select'uL' as ARTOutcome
			union all
		select'FV' as ARTOutcome
	)
	select 
		ARTOutcomeKey = IDENTITY(INT, 1, 1),
		ARTOutcome,
		case
			when ARTOutcome = 'S' then 'Stopped'
			when ARTOutcome = 'D' then 'Dead'
			when ARTOutcome = 'L' then 'Loss To Follow Up'
			when ARTOutcome = 'NV' then 'No Visit'
			when ARTOutcome = 'T' then 'Transferred Out'
			when ARTOutcome = 'V' then 'Active'
			when ARTOutcome = 'NP' then 'New Patient'
			when ARTOutcome = 'uL' then 'Undocumented Loss'
			when ARTOutcome = 'FV' then 'Future Visit'
		end as ARTOutcomeDescription,
		cast(getdate() as date) as LoadDate
	INTO [NDWH].[dbo].[DimARTOutcome]
	from distinct_ARTOutcomes;
	ALTER TABLE NDWH.dbo.DimARTOutcome ADD PRIMARY KEY(ARTOutcomeKey);
END