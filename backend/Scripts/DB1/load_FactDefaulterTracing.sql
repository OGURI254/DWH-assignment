IF OBJECT_ID(N'[NDWH].[dbo].[FactDefaulterTracing]', N'U') IS NOT NULL 
	DROP TABLE [NDWH].[dbo].[FactDefaulterTracing];
BEGIN


with MFL_partner_agency_combination as (
	select 
		distinct MFL_Code collate Latin1_General_CI_AS as MFL_Code,
		SDP collate Latin1_General_CI_AS as SDP,
	    SDP_Agency collate Latin1_General_CI_AS as Agency
	from ODS.dbo.All_EMRSites 
),
latest_differentiated_care as (
	select
		distinct visits.SiteCode,
		visits.PatientPKHash,
		visits.DifferentiatedCare
	from ODS.dbo.CT_PatientVisits as visits
	inner join ODS.dbo.Intermediate_LastVisitDate as last_visit on visits.SiteCode = last_visit.SiteCode 
		and visits.PatientPKHash = last_visit.PatientPKHash
		and visits.VisitDate = last_visit.LastVisitDate  
),
visits_data as (
    select
            defaulter_trace.PatientPKHash,
            defaulter_trace.PatientIDHash,
            defaulter_trace.SiteCode,
            VisitID,
            VisitDate,
            TracingType,
            TracingOutcome,
            IsFinalTrace,
            Comments,
            case 
                when TracingOutcome like '%No contact%' then 0
                else 1 
            end as is_reached,
            latest_differentiated_care.DifferentiatedCare collate Latin1_General_CI_AS as DifferentiatedCare,
            datediff(yy, patient.DOB, defaulter_trace.VisitDate) as AgeAtVisit
    from ODS.dbo.CT_DefaulterTracing as defaulter_trace
    left join ODS.dbo.CT_Patient as patient on patient.PatientPKHash = defaulter_trace.PatientPKHash
        and patient.SiteCode = defaulter_trace.SiteCode
	left join latest_differentiated_care on latest_differentiated_care.PatientPKHash = defaulter_trace.PatientPKHash
		and latest_differentiated_care.SiteCode = patient.SiteCode    
    where IsFinalTrace = 'Yes' and defaulter_trace.SiteCode >= 0
)
select 
    Factkey = IDENTITY(INT, 1, 1),
    patient.PatientKey,
	facility.FacilityKey,
	partner.PartnerKey,
	agency.AgencyKey,
    age_group.AgeGroupKey,
    visit.DateKey as VisitDateKey,
    diff_care.DifferentiatedCareKey,
    VisitID,
    TracingType,
    TracingOutcome,
    Comments,
    is_reached,
     cast(getdate() as date) as LoadDate
into NDWH.dbo.FactDefaulterTracing
from visits_data
left join NDWH.dbo.DimFacility as facility on facility.MFLCode = visits_data.SiteCode
left join NDWH.dbo.DimPatient as patient on patient.PatientPKHash collate Latin1_General_CI_AS = visits_data.PatientPKHash collate Latin1_General_CI_AS
    and patient.SiteCode = visits_data.SiteCode
left join MFL_partner_agency_combination on MFL_partner_agency_combination.MFL_Code = visits_data.SiteCode
left join NDWH.dbo.DimPartner as partner on partner.PartnerName = MFL_partner_agency_combination.SDP
left join NDWH.dbo.DimAgency as agency on agency.AgencyName = MFL_partner_agency_combination.Agency
left join NDWH.dbo.DimDate as visit on visit.Date = visits_data.VisitDate
left join NDWH.dbo.DimAgeGroup as age_group on age_group.Age = visits_data.AgeAtVisit
left join NDWH.dbo.DimDifferentiatedCare as diff_care on diff_care.DifferentiatedCare = visits_data.DifferentiatedCare;

alter table NDWH.dbo.FactDefaulterTracing add primary key(FactKey)

END