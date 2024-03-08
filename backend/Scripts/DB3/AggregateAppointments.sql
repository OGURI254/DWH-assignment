GO


select 
    facility.MFLCode,
    facility.FacilityName,
    facility.SubCounty,
    facility.County,
    partner.PartnerName,
    agency.AgencyName,
    patient.Gender,
    AppointmentStatus,
    count(*) NumOfPatients,
    AsOfDate,
    age_group.DATIMAgeGroup,
    cast(getdate() as date) as LoadDate
   into [REPORTING].[dbo].AggregateAppointments
from NDWH.dbo.FACTAppointments as apt
left join NDWH.dbo.DimFacility as facility on facility.FacilityKey = apt.FacilityKey
left join NDWH.dbo.DimPartner as partner on partner.PartnerKey = apt.PartnerKey
left join NDWH.dbo.DimPatient as patient on patient.PatientKey = apt.PatientKey
left join NDWH.dbo.DimAgency as agency on agency.AgencyKey = apt.AgencyKey
left join NDWH.dbo.DimAgeGroup as age_group on age_group.AgeGroupKey = DATEDIFF(YY,patient.DOB,apt.AsOfDate)

group by 
    facility.MFLCode,
    facility.FacilityName,
    facility.SubCounty,
    facility.County,
    partner.PartnerName,
    agency.AgencyName,
    patient.Gender,
    AppointmentStatus,
    AsOfDate,
    age_group.DATIMAgeGroup
