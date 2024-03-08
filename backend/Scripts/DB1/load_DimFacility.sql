---DimFacility Load
IF OBJECT_ID(N'[NDWH].[dbo].[DimFacility]', N'U') IS NOT NULL 
	DROP TABLE [NDWH].[dbo].[DimFacility];
BEGIN
	with source_facility as (
		select
			DISTINCT cast(MFL_Code as nvarchar) as MFLCode,
			Facility_Name as [FacilityName],
			SubCounty,
			County,
			EMR,		
			Project,
			Longitude,
			Latitude,
			Implementation,
			SDP_Agency As Agency
		from ODS.dbo.All_EMRSites
	),
	site_abstraction as (
		select
			SiteCode,
			max(VisitDate) as DateSiteAbstraction
		from ODS.dbo.CT_PatientVisits
		group by SiteCode
	),
	latest_upload as (
		select  
				SiteCode,
				max(cast([DateRecieved] as date)) as LatestDateUploaded
		from [ODS].[dbo].[CT_FacilityManifest](NoLock) 
		group by SiteCode
	)
	select 
		FacilityKey = IDENTITY(INT, 1, 1),
		source_facility.*,
		cast(format(site_abstraction.DateSiteAbstraction,'yyyyMMdd') as int) as DateSiteAbstractionKey,
		cast(format(latest_upload.LatestDateUploaded, 'yyyyMMdd') as int) as LatestDateUploadedKey,
		case when [Implementation] like '%CT%' then 1 else 0 end as isCT,
		case when [Implementation] like '%CT%' then 1 else 0 end as isPKV,
		case when [Implementation] like '%HTS%' then 1 else 0 end as isHTS,
		cast(getdate() as date) as LoadDate
	INTO [NDWH].[dbo].[DimFacility]
	from source_facility
	left join site_abstraction on site_abstraction.SiteCode = source_facility.MFLCode
	left join latest_upload on latest_upload.SiteCode = source_facility.MFLCode;

	ALTER TABLE NDWH.dbo.DimFacility ADD PRIMARY KEY(FacilityKey);

	with cte AS (
						Select
						MFLCode,
						
						 ROW_NUMBER() OVER (PARTITION BY MFLCode ORDER BY
						MFLCode) Row_Num
						FROM NDWH.dbo.DimFacility
						)
						delete from cte 
						Where Row_Num >1 ;
END