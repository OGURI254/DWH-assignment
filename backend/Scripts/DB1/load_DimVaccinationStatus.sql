IF OBJECT_ID(N'[NDWH].[dbo].[DimVaccinationStatus]', N'U') IS NOT NULL 
	DROP TABLE [NDWH].[dbo].[DimVaccinationStatus];
BEGIN
	with source_vaccination_status as (
		select
			distinct VaccinationStatus
		from ODS.dbo.CT_Covid
		where VaccinationStatus <> '' and VaccinationStatus is not null
	)
	select 
		VaccinationStatusKey = IDENTITY(INT, 1, 1),
		source_vaccination_status.*,
		cast(getdate() as date) as LoadDate
	into [NDWH].[dbo].[DimVaccinationStatus]
	from source_vaccination_status;

	alter table NDWH.dbo.DimVaccinationStatus add primary key(VaccinationStatusKey);

END