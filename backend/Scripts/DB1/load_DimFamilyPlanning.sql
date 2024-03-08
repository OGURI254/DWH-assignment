IF OBJECT_ID(N'[NDWH].[dbo].[DimFamilyPlanning]', N'U') IS NOT NULL 
	DROP TABLE [NDWH].[dbo].[DimFamilyPlanning];
BEGIN
	with source_FamilyPlanning as (
	select 
		distinct FamilyPlanningMethod as FamilyPlanning
	from ODS.dbo.CT_PatientVisits
	where FamilyPlanningMethod <> 'NULL' and FamilyPlanningMethod <>''
	)
	select
		FamilyPlanningKey = IDENTITY(INT, 1, 1),
		source_FamilyPlanning.*,
		cast(getdate() as date) as LoadDate
	into [NDWH].[dbo].[DimFamilyPlanning]
	from source_FamilyPlanning;

	ALTER TABLE NDWH.dbo.DimFamilyPlanning ADD PRIMARY KEY(FamilyPlanningKey);
END