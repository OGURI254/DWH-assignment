IF OBJECT_ID(N'[NDWH].[Dbo].[FactHTSClientLinkages]', N'U') IS NOT NULL 
	DROP TABLE [NDWH].[Dbo].[FactHTSClientLinkages];

BEGIN

with MFL_partner_agency_combination as (
	select 
		distinct MFL_Code,
		SDP,
        SDP_Agency as Agency
	from ODS.dbo.All_EMRSites 
),
source_data as (
select 
    SiteCode,
    PatientPK,
    EnrolledFacilityName,
    ReferralDate,
    DateEnrolled,
    DatePrefferedToBeEnrolled,
    FacilityReferredTo,
    HandedOverTo,
    HandedOverToCadre,
    convert(nvarchar(64), hashbytes('SHA2_256', cast(ReportedCCCNumber as nvarchar(36))), 2) as ReportedCCCNumber,
    row_number() over(partition by Sitecode,PatientPK order by DateEnrolled desc) as row_num
from ODS.dbo.HTS_ClientLinkages 
)
select 
    Factkey = IDENTITY(INT, 1, 1),
    patient.PatientKey,
    facility.FacilityKey,
    partner.PartnerKey,
    agency.AgencyKey,
    referral.DateKey as ReferralDateKey,
    enrolled.DateKey as DateEnrolledKey,
    preferred.DateKey as DatePrefferedToBeEnrolledKey,
    FacilityReferredTo,
    HandedOverTo,
    HandedOverToCadre,
    ReportedCCCNumber,
    cast(getdate() as date) as LoadDate
into NDWH.dbo.FactHTSClientLinkages
from source_data
left join NDWH.dbo.DimPatient as patient on patient.PatientPKHash = convert(nvarchar(64), hashbytes('SHA2_256', cast(source_data.PatientPK as nvarchar(36))), 2)
    and patient.SiteCode = source_data.SiteCode
left join NDWH.dbo.DimFacility as facility on facility.MFLCode = source_data.SiteCode
left join MFL_partner_agency_combination on MFL_partner_agency_combination.MFL_Code = source_data.SiteCode
left join NDWH.dbo.DimPartner as partner on partner.PartnerName = MFL_partner_agency_combination.SDP
left join NDWH.dbo.DimAgency as agency on agency.AgencyName = MFL_partner_agency_combination.Agency
left join NDWH.dbo.DimDate as referral on referral.Date = source_data.ReferralDate
left join NDWH.dbo.DimDate as enrolled on enrolled.Date = source_data.DateEnrolled
left join NDWH.dbo.DimDate as preferred on preferred.Date = source_data.DatePrefferedToBeEnrolled
where row_num = 1;

alter table NDWH.dbo.FactHTSClientLinkages add primary key(FactKey);

END