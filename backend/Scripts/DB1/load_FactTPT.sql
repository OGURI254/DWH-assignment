IF OBJECT_ID(N'[NDWH].[dbo].[FactTPT]', N'U') IS NOT NULL 
	DROP TABLE [NDWH].[dbo].[FactTPT];
BEGIN
with MFL_partner_agency_combination as (
	select 
		distinct MFL_Code,
		SDP,
	    SDP_Agency  as Agency
	from ODS.dbo.All_EMRSites
),
distinct_patients as (
    select 
        distinct PatientPk,
        SiteCode
    from  ODS.dbo.CT_IPT
),
date_started_TB_treatment as (
    select 
        distinct PatientPK,
        SiteCode,
        cast(TBRxStartDate as Date) as StartTBTreatmentDate 
    from ODS.dbo.CT_IPT
    where TBRxStartDate is not null
),
patient_TB_Diagnosis as (
    select 
        PatientPk,
        SiteCode, 
        min(ReportedbyDate) as TBDiagnosisDate
    from ODS.dbo.CT_PatientLabs 
    where (TestName like '%TB%' or TestName like '%Sput%' or TestName like '%TB%' or TestName like '%Tuber%')
    and  TestName <> 'SputumGramStain'
    and (TestResult like '%Positive%' or TestResult like '%HIGH%' or TestResult like '%+%')
    group by  
        PatientID,
        PatientPk,
        SiteCode
),
ipt_visits_ordered as (
    select 
        row_number() over (partition by PatientID, SiteCode, PatientPK order by VisitDate desc) as rank,
        PatientID,
        PatientPK,
        SiteCode,
        OnIPT,
        OnTBDrugs
    from ODS.dbo.CT_IPT
),
latest_visit as (
    select 
        *
    from ipt_visits_ordered 
    where rank = 1
),
combined_ipt_data as (
    select
       distinct_patients.PatientPK,
       patient.PatientPKHash,
       distinct_patients.SiteCode,
       date_started_TB_treatment.StartTBTreatmentDate,
       patient_TB_Diagnosis.TBDiagnosisDate,
       latest_visit.OnIPT,
        case 
            when latest_visit.OnTBDrugs = 'Yes' then 1
            else 0
        end as hasTB,
    datediff(yy, patient.DOB, last_encounter.LastEncounterDate) as AgeLastVisit
    from distinct_patients
    left join date_started_TB_treatment on date_started_TB_treatment.PatientPK = distinct_patients.PatientPK
        and date_started_TB_treatment.SiteCode = distinct_patients.SiteCode
    left join patient_TB_Diagnosis on patient_TB_Diagnosis.PatientPk = distinct_patients.PatientPK
        and patient_TB_Diagnosis.SiteCode = distinct_patients.SiteCode
    left join latest_visit on latest_visit.PatientPK = distinct_patients.PatientPK
        and latest_visit.SiteCode = distinct_patients.SiteCode
    left join ODS.dbo.Intermediate_LastPatientEncounter as last_encounter on last_encounter.PatientPK = distinct_patients.PatientPK
		and last_encounter.SiteCode = distinct_patients.SiteCode
    left join ODS.dbo.CT_Patient as patient on patient.PatientPK = distinct_patients.PatientPK
	and patient.SiteCode = distinct_patients.SiteCode
)
select 
    Factkey = IDENTITY(INT, 1, 1),
    patient.PatientKey,
	facility.FacilityKey,
	partner.PartnerKey,
	agency.AgencyKey,
    age_group.AgeGroupKey,
    tb_start_treatment.DateKey as StartTBTreatmentDateKey,
    tb_diagnosis.DateKey as TBDiagnosisDateKey,
    combined_ipt_data.OnIPT,
    combined_ipt_data.hasTB,
    cast(getdate() as date) as LoadDate
into NDWH.dbo.FactTPT
from combined_ipt_data
left join NDWH.dbo.DimPatient as patient on patient.PatientPKHash = combined_ipt_data.PatientPKHash
    and patient.SiteCode = combined_ipt_data.SiteCode
left join NDWH.dbo.DimFacility as facility on facility.MFLCode = combined_ipt_data.SiteCode
left join NDWH.dbo.DimDate as tb_start_treatment on tb_start_treatment.Date = combined_ipt_data.StartTBTreatmentDate
left join NDWH.dbo.DimDate as tb_diagnosis on tb_diagnosis.Date = combined_ipt_data.TBDiagnosisDate
left join MFL_partner_agency_combination on MFL_partner_agency_combination.MFL_Code = combined_ipt_data.SiteCode
left join NDWH.dbo.DimPartner as partner on partner.PartnerName = MFL_partner_agency_combination.SDP
left join NDWH.dbo.DimAgency as agency on agency.AgencyName = MFL_partner_agency_combination.Agency
left join NDWH.dbo.DimAgeGroup as age_group on age_group.Age = combined_ipt_data.AgeLastVisit;

alter table NDWH.dbo.FactTPT add primary key(FactKey);
END
