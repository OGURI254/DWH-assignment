IF OBJECT_ID(N'[NDWH].[dbo].[DimPartner]', N'U') IS NOT NULL 
	DROP TABLE [NDWH].[dbo].[DimPartner];
BEGIN
	---DimPartner Load
	with source_partner as (
	select
		distinct SDP as PartnerName
	from [ODS].[dbo].[All_EMRSites](NoLock)
	)
	select 
		PartnerKey = IDENTITY(INT, 1, 1),
		source_partner.*,
		cast(getdate() as date) as LoadDate
	INTO [NDWH].[dbo].[DimPartner]
	from source_partner;

	ALTER TABLE NDWH.dbo.DimPartner ADD PRIMARY KEY(PartnerKey);
END