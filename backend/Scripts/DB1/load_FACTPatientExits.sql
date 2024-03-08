IF OBJECT_ID(N'[NDWH].[dbo].[FACTPatientExits]', N'U') IS NOT NULL 
	DROP TABLE [NDWH].[dbo].[FACTPatientExits];
BEGIN
	With Exits As (
	select 
		PatientID,
		PatientPKHash,
		SiteCode
	from ODS.dbo.CT_PatientStatus
	where ExitReason is not null
	),
	Died As (select 
		PatientID,
		PatientPKHash,
		SiteCode,
		ExitDate as dtDead
	from ODS.dbo.CT_PatientStatus
	where ExitReason in ('Died','death')
	),

	Stopped As (select 
		PatientID,
		PatientPKHash,
		SiteCode,
		ExitDate as dtARTStop
	from ODS.dbo.CT_PatientStatus
	where ExitReason in ('Stopped','Stopped Treatment')
	),
	LTFU AS ( Select
		PatientID,
		PatientPKHash,
		SiteCode,
		ExitDate as dtLTFU
	from ODS.dbo.CT_PatientStatus
	where ExitReason in ('Lost','Lost to followup','LTFU')
	),  
	TransferOut AS ( Select
		PatientID,
		PatientPK,
		SiteCode,
		ExitDate as dtTO
	from ODS.dbo.CT_PatientStatus
	where ExitReason in ('Transfer Out','transfer_out','Transferred out','Transfer')
	),
	MFL_partner_agency_combination As (
	Select 
		MFL_code,
		SDP,
		[SDP_Agency]  as agency
		from [ODS].dbo.All_EMRSites
		)

	Select 
		FACTKey= IDENTITY (INT,1,1),
		Patient.PatientKey,
		facility.FacilityKey,
		partner.PartnerKey,
		agency.AgencyKey,
		dtDead.DateKey As dtDeadKey,
		dtLTFU.DateKey As dtLFTUKey,
		dtTO.DateKey As dtTOKey,
		dtARTStop.DateKey As dtARTStopKey,
		cast (getdate() as date) as LoadDate
		INTO [NDWH].[DBO].[FACTPatientExits]
		from Exits
		Left join NDWH.dbo.DimPatient as Patient on Patient.PatientPKHash= Exits.PatientPKHash and Patient.SiteCode=Exits.SiteCode
		Left join NDWH.dbo.DimFacility as facility on facility.MFLCode=Exits.SiteCode
		left join MFL_partner_agency_combination on MFL_partner_agency_combination.MFL_Code=Exits.SiteCode
		Left join NDWH.dbo.DimPartner as partner on partner.PartnerName=MFL_partner_agency_combination.SDP
		Left join NDWH.dbo.DimAgency as agency on agency.AgencyName=MFL_partner_agency_combination.agency
		left join Died on Died.PatientPKHash=Exits.PatientPKHash and Died.SiteCode=Exits.SiteCode
		left join [Stopped] on [Stopped].PatientPKHash=Exits.PatientPKHash and [Stopped].SiteCode=Exits.SiteCode
		left join TransferOut on TransferOut.PatientPK=Exits.PatientPKHash and TransferOut.SiteCode=Exits.SiteCode
		left join LTFU on LTFU.PatientPKHash=Exits.PatientPKHash and LTFU.SiteCode=Exits.SiteCode
		left join NDWH.dbo.DimDate as dtARTStop on dtARTStop.Date=Stopped.dtARTStop
		left join NDWH.dbo.DimDate as dtLTFU on dtLTFU.Date= LTFU.dtLTFU
		left join NDWH.dbo.DimDate as dtTO on dtTO.Date= TransferOut.dtTO
		left join NDWH.dbo.DimDate as dtDead on dtDead.Date= Died.dtDead;

		alter table [NDWH].[DBO].[FACTPatientExits] add primary key (FACTKey);
END