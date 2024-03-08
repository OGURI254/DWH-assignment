IF OBJECT_ID(N'[NDWH].[dbo].[DimDifferentiatedCare]', N'U') IS NOT NULL 
	DROP TABLE [NDWH].[dbo].[DimDifferentiatedCare];
BEGIN
	--DimDifferentiatedCare
	with source_DifferentiatedCare as (
	select 
		distinct DifferentiatedCare as DifferentiatedCare
	from ODS.dbo.CT_PatientVisits
	where DifferentiatedCare <> 'NULL' and DifferentiatedCare <>''
	)
	select
		DifferentiatedCareKey = IDENTITY(INT, 1, 1),
		source_DifferentiatedCare.*,
		cast(getdate() as date) as LoadDate
	into [NDWH].[dbo].[DimDifferentiatedCare]
	from source_DifferentiatedCare;
	ALTER TABLE NDWH.dbo.DimDifferentiatedCare ADD PRIMARY KEY(DifferentiatedCareKey);
END