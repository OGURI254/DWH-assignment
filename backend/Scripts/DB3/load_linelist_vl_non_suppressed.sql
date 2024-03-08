GO

SELECT DISTINCT
	PatientIDHash,
    PatientPKHash,
    MFLCode,
	f.FacilityName,
	SubCounty,
	County,
	p.PartnerName,
	a.AgencyName,
	pat.Gender,
	g.DATIMAgeGroup as AgeGroup,
	art.AgeLastVisit,
	StartARTDateKey as StartARTDate,
	ValidVLResult,
	art.LastVisitDate,
	art.NextAppointmentDate,
	aro.ARTOutcome,
    CAST(GETDATE() AS DATE) AS LoadDate ,
	case 
		when aro.ARTOutcome is null then 'Others'
		else aro.ARTOutcomeDescription
	end as ARTOutcomeDescription
INTO [REPORTING].[dbo].[LineListVLNonSuppressed]
FROM NDWH.dbo.FactViralLoads it
INNER join NDWH.dbo.DimAgeGroup g on g.AgeGroupKey=it.AgeGroupKey
INNER join NDWH.dbo.DimFacility f on f.FacilityKey = it.FacilityKey
INNER JOIN NDWH.dbo.DimAgency a on a.AgencyKey = it.AgencyKey
INNER JOIN NDWH.dbo.DimPatient pat on pat.PatientKey = it.PatientKey
INNER JOIN NDWH.dbo.DimPartner p on p.PartnerKey = it.PartnerKey
INNER JOIN NDWH.dbo.FactART art on art.PatientKey = it.PatientKey
INNER JOIN NDWH.dbo.DimARTOutcome aro on aro.ARTOutcomeKey = art.ARTOutcomeKey
WHERE ValidVLResultCategory1 in ('>1000', '200-999')