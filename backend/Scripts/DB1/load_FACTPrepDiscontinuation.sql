IF OBJECT_ID(N'[NDWH].[dbo].[FactPrepDiscontinuation]', N'U') IS NOT NULL 
DROP TABLE [NDWH].[dbo].[FactPrepDiscontinuation];

BEGIN

    with MFL_partner_agency_combination as (
        select 
            distinct MFL_Code,
            SDP,
        SDP_Agency  as Agency
        from ODS.dbo.All_EMRSites 
    ),
    prep_patients as
    (
        select
            PatientPKHash,
            SiteCode
        from ODS.dbo.PrEP_Patient
        where ODS.dbo.PrEP_Patient.PrepNumber is not null
    ),

PrepDiscontinuation as (
        select 
             PatientPKHash,
                SiteCode,
                ExitDate,
                ExitReason                  
        from ODS.DBO.PrEP_CareTermination
        

    )


    select 
        FactKey = IDENTITY(INT, 1, 1),
        patient.PatientKey,
        facility.FacilityKey,
        agency.AgencyKey,
        partner.PartnerKey,
        age_group.AgeGroupKey,
        Discontinuation.DateKey as ExitDateKey,
        PrepDiscontinuation.ExitDate,
        PrepDiscontinuation.ExitReason,
        cast(getdate() as date) as LoadDate
    into NDWH.dbo.FactPrepDiscontinuation
    from prep_patients
    left join PrepDiscontinuation on PrepDiscontinuation.PatientPKHash =  prep_patients.PatientPKHash
        and PrepDiscontinuation.SiteCode = prep_patients.SiteCode
    left join NDWH.dbo.DimPatient as patient on patient.PatientPKHash = prep_patients.PatientPKHash
        and patient.SiteCode = prep_patients.SiteCode
    left join MFL_partner_agency_combination on MFL_partner_agency_combination.MFL_Code = prep_patients.SiteCode
    left join NDWH.dbo.DimPartner as partner on partner.PartnerName = MFL_partner_agency_combination.SDP 
    left join NDWH.dbo.DimAgency as agency on agency.AgencyName = MFL_partner_agency_combination.Agency
    left join NDWH.dbo.DimFacility as facility on facility.MFLCode = prep_patients.SiteCode
    left join NDWH.dbo.DimDate as Discontinuation on Discontinuation.Date = PrepDiscontinuation.ExitDate
    left join NDWH.dbo.DimAgeGroup as age_group on age_group.Age = datediff(yy, patient.DOB, PrepDiscontinuation.ExitDate);

    alter table NDWH.dbo.FactPrepDiscontinuation add primary key(FactKey);
END