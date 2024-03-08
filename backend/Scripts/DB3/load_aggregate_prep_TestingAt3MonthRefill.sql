GO

SELECT DISTINCT 
	MFLCode,		
	f.FacilityName,
	County,
	SubCounty,
	p.PartnerName,
	a.AgencyName,
	pat.Gender,
	test.Month,
	test.Year,
    EOMONTH(test.Date) as AsofDate,
	age.DATIMAgeGroup as AgeGroup,
	sum(case when DateDispenseMonth3 is not null then 1 else 0 end) refilled,
	sum(case when TestResultsMonth3 is not null then 1 else 0 end) tested,
	sum(case when TestResultsMonth3 is null then 1 else 0 end) nottested,
    CAST(GETDATE() AS DATE) AS LoadDate 
INTO REPORTING.dbo.AggregatePrepTestingAt3MonthRefill
FROM NDWH.dbo.FactPrepRefills prep
LEFT JOIN NDWH.dbo.DimFacility f on f.FacilityKey = prep.FacilityKey
LEFT JOIN NDWH.dbo.DimAgency a on a.AgencyKey = prep.AgencyKey
LEFT JOIN NDWH.dbo.DimPatient pat on pat.PatientKey = prep.PatientKey
LEFT join NDWH.dbo.DimAgeGroup age on age.AgeGroupKey=prep.AgeGroupKey
LEFT JOIN NDWH.dbo.DimPartner p on p.PartnerKey = prep.PartnerKey
LEFT JOIN NDWH.dbo.DimDate test ON test.DateKey = DateDispenseMonth3 
GROUP BY  
    MFLCode,		
	f.FacilityName,
	County,
	SubCounty,
	p.PartnerName,
	a.AgencyName,
	pat.Gender,
	age.DATIMAgeGroup,
	test.Month,
	test.Year,
    EOMONTH(test.Date)