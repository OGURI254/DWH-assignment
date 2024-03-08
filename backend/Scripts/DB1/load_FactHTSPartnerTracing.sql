IF OBJECT_ID(N'[NDWH].[dbo].[FactHTSPartnerTracing]', N'U') IS NOT NULL 
	DROP TABLE [NDWH].[dbo].[FactHTSPartnerTracing];

BEGIN

    with MFL_partner_agency_combination as (
        select 
            distinct MFL_Code,
            SDP,
            SDP_Agency  as Agency
        from ODS.dbo.All_EMRSites 
    ),
    source_data as (
        select 
            PatientPK,
            SiteCode,
            TraceType,
            TraceDate,
            TraceOutcome,
            cast(BookingDate as date) as BookingDate
        from ODS.dbo.HTS_PartnerTracings
    )
    select 
        Factkey = IDENTITY(INT, 1, 1),
        patient.PatientKey,
        facility.FacilityKey,
        partner.PartnerKey,
        agency.AgencyKey,
        outcome.TraceOutcomeKey,
        trace_type.TraceTypeKey,
        booking.DateKey as BookingDateKey,
        cast(getdate() as date) as LoadDate
    into NDWH.dbo.FactHTSPartnerTracing
    from source_data
    left join NDWH.dbo.DimPatient as patient on patient.PatientPKHash = convert(nvarchar(64), hashbytes('SHA2_256', cast(source_data.PatientPK as nvarchar(36))), 2)
        and patient.SiteCode = source_data.SiteCode
    left join NDWH.dbo.DimFacility as facility on facility.MFLCode = source_data.SiteCode
    left join MFL_partner_agency_combination on MFL_partner_agency_combination.MFL_Code = source_data.SiteCode
    left join NDWH.dbo.DimPartner as partner on partner.PartnerName = MFL_partner_agency_combination.SDP
    left join NDWH.dbo.DimAgency as agency on agency.AgencyName = MFL_partner_agency_combination.Agency
    left join NDWH.dbo.DimDate as booking on booking.Date = source_data.BookingDate
    left join NDWH.dbo.DimHTSTraceOutcome as outcome on outcome.TraceOutcome = source_data.TraceOutcome
    left join NDWH.dbo.DimHTSTraceType as trace_type on trace_type.TraceType = source_data.TraceType;


    alter table NDWH.dbo.FactHTSPartnerTracing add primary key(FactKey);

END




