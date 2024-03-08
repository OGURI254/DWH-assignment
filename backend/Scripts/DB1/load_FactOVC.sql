IF OBJECT_ID(N'[NDWH].[dbo].[FactOVC]', N'U') IS NOT NULL 
	DROP TABLE [NDWH].[dbo].[FactOVC];
BEGIN	
with MFL_partner_agency_combination as (
	select 
		distinct MFL_Code,
		SDP,
	    SDP_Agency  as Agency
	from ODS.dbo.All_EMRSites 
),
source_ovc as (
	select
		distinct 
		ovc.PatientIDHash,
		row_number () over (partition by ovc.SiteCode, ovc.PatientPK order by ovc.OVCEnrollmentDate desc) as rank,
		ovc.PatientPKHash,
		ovc.SiteCode,
		OVCEnrollmentDate,
		RelationshipToClient,
		EnrolledinCPIMS,
		CPIMSUniqueIdentifierHash,
		PartnerOfferingOVCServices,
		OVCExitReason,
		ExitDate,
		datediff(yy, patient.DOB, last_encounter.LastEncounterDate) as AgeLastVisit
	from ODS.dbo.CT_OVC as ovc
	left join ODS.dbo.Intermediate_LastPatientEncounter as last_encounter on last_encounter.PatientPKHash = ovc.PatientPKHash
		and last_encounter.SiteCode = ovc.SiteCode
	left join ODS.dbo.CT_Patient as patient on patient.PatientPKHash = ovc.PatientPKHash
	and patient.SiteCode = ovc.SiteCode
)
select 
	Factkey = IDENTITY(INT, 1, 1),
	patient.PatientKey,
	facility.FacilityKey,
	partner.PartnerKey,
	agency.AgencyKey,
    age_group.AgeGroupKey,
	ovc_enrollment.DateKey as OVCEnrollmentDateKey,
	relationship_client.RelationshipWithPatientKey,
	source_ovc.EnrolledinCPIMS,
	CPIMSUniqueIdentifierHash,
	PartnerOfferingOVCServices,
	OVCExitReason,
	exit_date.DateKey as OVCExitDateKey,
	cast(getdate() as date) as LoadDate
into NDWH.dbo.FactOVC
from source_ovc
left join NDWH.dbo.DimPatient as patient on patient.PatientPKHash = source_ovc.PatientPKHash
    and patient.SiteCode = source_ovc.SiteCode
left join NDWH.dbo.DimFacility as facility on facility.MFLCode = source_ovc.SiteCode
left join NDWH.dbo.DimDate as ovc_enrollment on ovc_enrollment.Date = source_ovc.OVCEnrollmentDate
left join NDWH.dbo.DimDate as exit_date on exit_date.Date = source_ovc.ExitDate
left join MFL_partner_agency_combination on MFL_partner_agency_combination.MFL_Code = source_ovc.SiteCode
left join NDWH.dbo.DimPartner as partner on partner.PartnerName = MFL_partner_agency_combination.SDP
left join NDWH.dbo.DimAgency as agency on agency.AgencyName = MFL_partner_agency_combination.Agency
left join NDWH.dbo.DimAgeGroup as age_group on age_group.Age = source_ovc.AgeLastVisit
left join NDWH.dbo.DimRelationshipWithPatient as relationship_client on relationship_client.RelationshipWithPatient = source_ovc.RelationshipToClient
where source_ovc.rank = 1;

alter table NDWH.dbo.FactOVC add primary key(FactKey);
END