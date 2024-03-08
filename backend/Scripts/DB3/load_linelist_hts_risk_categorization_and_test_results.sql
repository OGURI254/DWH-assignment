GO

BEGIN

with source_data as (
	select 
		row_number() over (partition by tests.FacilityKey, tests.PatientKey, tests.DateTestedKey, tests.TestType order by tests.DateTestedKey desc) as num,
		patient.PatientPKHash,
		patient.DOB,
		patient.Gender,
		facility.FacilityName,
		facility.County,
		facility.SubCounty,
		facility.MFLCode,
		partner.PartnerName,
		agency.AgencyName,
		facility.latitude,
		facility.longitude,
		testDate.Date as TestDate,
		elig.VisitDateKey,
		elig.HIVRiskCategory,
		elig.HtsRiskScore,
		tests.FinalTestResult as HTSResult,
		elig.ReasonRefferredForTesting
	from 
		NDWH.dbo.FactHTSClientTests as tests
	left join NDWH.dbo.FactHTSEligibilityExtract elig on elig.PatientKey = tests.PatientKey
		and elig.VisitDateKey = tests.DateTestedKey
	left join NDWH.dbo.DimFacility as facility on facility.FacilityKey = tests.FacilityKey
	left join NDWH.dbo.DimAgency as agency on agency.AgencyKey = tests.AgencyKey
	left join NDWH.dbo.DimPartner as partner on partner.PartnerKey = tests.PartnerKey
	left join NDWH.dbo.DimPatient as patient on patient.PatientKey = tests.PatientKey
	left join NDWH.dbo.DimDate as testDate on testDate.DateKey =tests.DateTestedKey
	where 
		HIVRiskCategory is not null
)
select 
		PatientPKHash,
		DOB,
		Gender,
		FacilityName,
		County,
		SubCounty,
		MFLCode,
		PartnerName,
		AgencyName,
		latitude,
		longitude,
		TestDate,
		VisitDateKey,
		HIVRiskCategory,
		HtsRiskScore,
		HTSResult,
		ReasonRefferredForTesting
into REPORTING.dbo.LineListHTSRiskCategorizationAndTestResults
from source_data
where num = 1

END