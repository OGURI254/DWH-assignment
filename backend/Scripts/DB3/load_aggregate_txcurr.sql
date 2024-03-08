GO


select 
    facility.MFLCode,
    facility.FacilityName,
    facility.SubCounty,
    facility.County,
    partner.PartnerName,
    agency.AgencyName,
    patient.Gender,
    age_group.DATIMAgeGroup,
    count(*) as CountClientsTXCur,
    CAST(GETDATE() AS DATE) AS LoadDate   
  into [REPORTING].[dbo].AggregateTXCurr
from NDWH.dbo.FactArt as art
left join NDWH.dbo.DimFacility as facility on facility.FacilityKey = art.FacilityKey
left join NDWH.dbo.DimPartner as partner on partner.PartnerKey = art.PartnerKey
left join NDWH.dbo.DimPatient as patient on patient.PatientKey = art.PatientKey
left join NDWH.dbo.DimAgeGroup as age_group on age_group.AgeGroupKey = art.AgeGroupKey
left join NDWH.dbo.DimAgency as agency on agency.AgencyKey = art.AgencyKey
left join NDWH.dbo.DimARTOutcome as outcome on outcome.ARTOutcomeKey = art.ARTOutcomeKey
where outcome.ARTOutcomeDescription = 'Active'
group by 
    facility.MFLCode,
    facility.FacilityName,
    facility.SubCounty,
    facility.County,
    partner.PartnerName,
    agency.AgencyName,
    patient.Gender,
    age_group.DATIMAgeGroup
