GO

with pns_and_tests as ( 
	select distinct 
		pns.PatientKey,
		facility.MFLCode,
        facility.FacilityKey,
        pns.AgencyKey,
        pns.PartnerKey,
        pns.AgeGroupKey,
		pns.ScreenedForIpv,
		pns.CccNumber,
        pns.RelationsipToIndexClient,
        pns.KnowledgeOfHivStatus,
		tests.FinalTestResult,
		pns.DateElicitedKey,
        pns.DateLinkedToCareKey,
		tests.DateTestedKey
	from NDWH.dbo.FactHTSPartnerNotificationServices as pns
    left join NDWH.dbo.DimFacility as facility on facility.FacilityKey = pns.FacilityKey
	left join NDWH.dbo.DimPatient as patient on patient.PatientPKHash = pns.PartnerPatientPk
        and patient.SiteCode = facility.MFLCode
    left join NDWH.dbo.FactHTSClientTests as tests on tests.PatientKey = patient.PatientKey
    where 
		TestType in ('Initial Test', 'Initial') and 
		pns.RelationsipToIndexClient in ('Child') 
),
pns_tests_linkages as ( 
select 
    pns_and_tests.*,
    linkages.ReportedCCCNumber  
from pns_and_tests
left join NDWH.dbo.FactHTSClientLinkages as linkages on linkages.PatientKey = pns_and_tests.PatientKey
),
line_list_dataset as (
	select distinct
        dataset.PatientKey,
		facility.MFLCode,
		facility.FacilityName,
		facility.County,
		facility.SubCounty,
		patner.PartnerName,
		agency.AgencyName,
		RelationsipToIndexClient, 
		FinalTestResult,
		elicited.Date as DateElicited,
		tested.Date as TestDate,
		tested.year,
		tested.month,
		FORMAT(cast(tested.Date as date), 'MMMM') MonthName, 
        EOMONTH(tested.date) as AsOfDate,
		Gender,
		DATIMAgeGroup as Agegroup,
		case 
			when (dataset.PatientKey is not null) then 1 
		    else 0 
        end  elicited,
		case 
			when (FinalTestResult is not null ) then 1
		    else 0 
        end as tested,
		case 
			when (FinalTestResult = 'Positive' ) then 1
		    else 0 
        end as positive,        
		case 
			when (FinalTestResult = 'Positive' and ReportedCCCNumber is not null ) then 1 
		    else 0 
        end  Linked,
		case 
			when (KnowledgeOfHivStatus = 'Positive') then 1 
		    else 0 
        end as  KP    
	from  pns_tests_linkages as dataset
	left join NDWH.dbo.DimPatient as patient ON patient.PatientKey = dataset.PatientKey
	left join NDWH.dbo.DimPartner as patner ON patner.PartnerKey = dataset.PartnerKey
	left join NDWH.dbo.DimFacility as facility  ON facility.FacilityKey = dataset.FacilityKey
	left join NDWH.dbo.DimAgency agency ON agency.AgencyKey = dataset.AgencyKey	
	left join NDWH.dbo.DimDate as elicited on elicited.DateKey = dataset.DateElicitedKey
	left join NDWH.dbo.DimDate as tested on tested.DateKey = dataset.DateTestedKey
	left join NDWH.dbo.DimDate linked on linked.DateKey = dataset.DateLinkedToCareKey
	left join NDWH.dbo.DimAgeGroup as agegroup on agegroup.AgeGroupKey = dataset.AgeGroupKey

)
select 
	Mflcode, 
	FacilityName, 
	County, 
	SubCounty, 
	PartnerName, 
	AgencyName,
	Gender,
	Agegroup,
	year,
	month,
	MonthName,
    AsOfDate, 
	sum(elicited) as ChildrenElicited,
	sum(tested) as ChildrenTested,
	sum(positive) as ChildrenPositive,
    sum(Linked) as ChildrenLinked,
	sum(KP) as ChildrenKnownPositive,
    CAST(GETDATE() AS DATE) AS LoadDate  
    into REPORTING.dbo.AggregateHTSPNSChildren
from line_list_dataset
group by 
    Mflcode,
    FacilityName,
    County,
    subcounty,
    PartnerName,
    year,
    month,
    monthName,
    AsOfDate,
    Gender,
    Agegroup,
    AgencyName;