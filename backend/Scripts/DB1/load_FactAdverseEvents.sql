IF OBJECT_ID(N'[NDWH].[dbo].[FactAdverseEvents]', N'U') IS NOT NULL 
	DROP TABLE [NDWH].[dbo].[FactAdverseEvents];
BEGIN


with MFL_partner_agency_combination as (
	select 
		distinct MFL_Code,
		SDP ,
	    SDP_Agency  as Agency
	from ODS.dbo.All_EMRSites 
),
source_data as (
    select 
        distinct adverse_events.Patientpk,
        adverse_events.PatientPKHash,
        adverse_events.SiteCode,
        AdverseEvent,
        AdverseEventStartDate,
        AdverseEventEndDate,
        Severity,
        VisitDate,
        adverse_events.EMR,
        AdverseEventCause,
        AdverseEventRegimen,
        AdverseEventActionTaken,
        AdverseEventClinicalOutcome,
        AdverseEventIsPregnant,
        datediff(yy, patient.DOB, last_encounter.LastEncounterDate) as AgeLastVisit
    from ODS.dbo.CT_AdverseEvents as adverse_events
    left join ODS.dbo.CT_Patient as patient on patient.PatientPK = adverse_events.PatientPK
        and patient.SiteCode = adverse_events.SiteCode
    left join ODS.dbo.Intermediate_LastPatientEncounter as last_encounter on last_encounter.PatientPK = adverse_events.PatientPK
        and last_encounter.SiteCode = adverse_events.SiteCode
)
select 
    Factkey = IDENTITY(INT, 1, 1),
    patient.PatientKey,
	facility.FacilityKey,
	partner.PartnerKey,
	agency.AgencyKey,
    age_group.AgeGroupKey,
    adverse_event_start.DateKey as AdverseEventStartDateKey,
    adverse_event_end.DateKey as AdverseEventEndDateKey,
    visit.DateKey as VisitDateKey,
    source_data.AdverseEvent,
    source_data.Severity,
    source_data.AdverseEventCause,
    source_data.AdverseEventRegimen,
    source_data.AdverseEventActionTaken,
    source_data.AdverseEventClinicalOutcome,
    source_data.AdverseEventIsPregnant,
    cast(getdate() as date) as LoadDate
into [NDWH].[dbo].[FactAdverseEvents]
from source_data
left join NDWH.dbo.DimFacility as facility on facility.MFLCode  = source_data.SiteCode
left join NDWH.dbo.DimPatient as patient on patient.PatientPKHash = source_data.PatientPKHash
     and patient.SiteCode = source_data.SiteCode
left join MFL_partner_agency_combination on MFL_partner_agency_combination.MFL_Code = source_data.SiteCode
left join NDWH.dbo.DimPartner as partner on partner.PartnerName = MFL_partner_agency_combination.SDP
left join NDWH.dbo.DimAgency as agency on agency.AgencyName = MFL_partner_agency_combination.Agency
left join NDWH.dbo.DimDate as adverse_event_start on adverse_event_start.Date = source_data.AdverseEventStartDate
left join NDWH.dbo.DimDate as adverse_event_end on adverse_event_end.Date = source_data.AdverseEventEndDate
left join NDWH.dbo.DimDate as visit on visit.Date = source_data.VisitDate
left join NDWH.dbo.DimAgeGroup as age_group on age_group.Age = source_data.AgeLastVisit;

alter table NDWH.dbo.FactAdverseEvents add primary key(FactKey)

END