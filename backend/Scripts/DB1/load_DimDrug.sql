IF OBJECT_ID(N'[NDWH].[dbo].[DimDrug]', N'U') IS NOT NULL 
	DROP TABLE [NDWH].[dbo].[DimDrug];
BEGIN
	--DimDrug
	with source_Drug as (
	select 
		distinct Drug as Drug
	from ODS.dbo.CT_PatientPharmacy
	where Drug <> 'NULL' and Drug <>'' and TreatmentType='ARV'
	)
	select
		DrugKey = IDENTITY(INT, 1, 1),
		source_Drug.*,
		cast(getdate() as date) as LoadDate
	into [NDWH].[dbo].[DimDrug]
	from source_Drug;
	ALTER TABLE [NDWH].dbo.DimDrug ADD PRIMARY KEY(DrugKey);
END