IF OBJECT_ID(N'[NDWH].[dbo].FACTAppointments', N'U') IS NOT NULL 		
	drop table [NDWH].[dbo].FACTAppointments
GO
With Patients As (
select 
     MFLCode,
     AppointmentStatus,
    count(*) NumOfPatients,
    AsOfDate
from ODS.dbo.HistoricalAppointmentStatus as apt
group by 
    MFLCode,
    AppointmentStatus,
    AsOfDate
),
MFL_partner_agency_combination as (
		select 
			distinct MFL_Code,
			SDP,
			SDP_Agency as Agency
		from ODS.dbo.All_EMRSites 
)
Select 
        FactKey = IDENTITY(INT, 1, 1),
		facility.FacilityKey,
		partner.PartnerKey,
		agency.AgencyKey,
		patient.PatientKey,
		as_of.DateKey as AsOfDateKey,
        patient.Gender,
		LastEncounterDate,
		ExpectedNextAppointmentDate,
		AppointmentStatus,
		DiffExpectedTCADateLastEncounter,
        AsofDate,
        cast(getdate() as date) as LoadDate
        into NDWH.dbo.FACTAppointments
        from ODS.dbo.[HistoricalAppointmentStatus] as apt
left join NDWH.dbo.DimFacility as facility on facility.MFLCode = apt.MFLCode
left join MFL_partner_agency_combination on MFL_partner_agency_combination.MFL_Code=apt.MFLCode
left join NDWH.dbo.DimPartner as partner on partner.PartnerName = MFL_partner_agency_combination.SDP
left join NDWH.dbo.DimPatient as patient on patient.PatientPKHash = apt.PatientPKhash and patient.SiteCode=apt.MFLCode
left join NDWH.dbo.DimAgency as agency on agency.AgencyName = MFL_partner_agency_combination.Agency
left join NDWH.dbo.DimAgeGroup as age_group on age_group.AgeGroupKey = DATEDIFF(YY,patient.DOB,apt.AsOfDate)
left join NDWH.dbo.DimDate as as_of on as_of.Date = apt.AsOfDate


	alter table NDWH.dbo.FACTAppointments add primary key(FactKey);


