IF OBJECT_ID(N'[NDWH].[dbo].[DimAgency]', N'U') IS NOT NULL 
	DROP TABLE  [NDWH].[dbo].[DimAgency];
BEGIN--DimAgency Load
	with source_agency as (
	select 
		distinct [SDP_Agency] as AgencyName
	from ODS.dbo.All_EMRSites
	where [SDP_Agency] <> 'NULL'
	)
	select
		AgencyKey = IDENTITY(INT, 1, 1),
		source_agency.*,
		cast(getdate() as date) as LoadDate
	into [NDWH].[dbo].[DimAgency]
	from source_agency;

	ALTER TABLE NDWH.dbo.DimAgency ADD PRIMARY KEY(AgencyKey);
END