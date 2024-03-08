IF OBJECT_ID(N'[NDWH].[dbo].[FactManifiest]', N'U') IS NOT NULL 
	DROP TABLE [NDWH].[dbo].[FactManifiest];
BEGIN
    with Uploads As (
        Select 
            cast(DateRecieved as date) as Dateuploaded,
            cast([Start] as date) as [Start],
            cast([End] as date) as [End],
            SiteCode
        from ODS.dbo.CT_FacilityManifest
        ),

        MFL_partner_agency_combination as (

            SELECT distinct 
            MFL_Code,
            SDP,
            [SDP_Agency] collate Latin1_General_CI_AS as Agency
            from ODS.dbo.[All_EMRSites]
    )
    Select 
        FactKey= IDENTITY(INT,1,1),
        Uploaddates.DateKey as UploadsDateKey,
        facility.FacilityKey ,
        partner.PartnerKey,
        agency.AgencyKey,
        started.DateKey as StartDateKey,
        ended.DateKey as EndDateKey,
        cast (getdate() as date) as LoadDate
    INTO NDWH.dbo.FactManifiest
    from Uploads
    left join NDWH.dbo.DimFacility as facility on facility.MFLCode=Uploads.Sitecode
    left join MFL_partner_agency_combination on MFL_partner_agency_combination.MFL_Code=Uploads.SiteCode
    left join NDWH.dbo.DimPartner as partner on partner.PartnerName=MFL_partner_agency_combination.SDP collate Latin1_General_CI_AS
    left join NDWH.dbo.DimAgency as agency on Agency.AgencyName=MFL_partner_agency_combination.Agency collate Latin1_General_CI_AS
    left join NDWH.dbo.DimDate as UploadDates on UploadDates.Date = Uploads.Dateuploaded
    left join NDWH.dbo.DimDate as started on started.Date = Uploads.[Start]
    left join NDWH.dbo.DimDate as ended on ended.Date = Uploads.[End]

    alter table NDWH.dbo.FactManifiest add primary key(FactKey)

END
