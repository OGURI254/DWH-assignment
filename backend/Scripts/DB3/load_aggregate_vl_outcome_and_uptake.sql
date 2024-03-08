GO

SELECT
	MFLCode,
	f.FacilityName,
	County,
	SubCounty,
	p.PartnerName,
	a.AgencyName,
	pat.Gender,
	YEAR (art.StartARTDateKey ) AS StartARTYear,
    EOMONTH(date.Date) as AsOfDate,
	g.DATIMAgeGroup AS AgeGroup,
	COUNT (vl.ValidVLResultCategory2 ) AS TotalValidVLResultCategory,
	case 
		when vl.ValidVLResultCategory2 in  ('Low Risk LLV', 'LDL') then 'SUPPRESSED'
		else vl.ValidVLResultCategory2
	end as ValidVLResultCategory,
	SUM ( IsTXCurr ) AS TXCurr,
	SUM ( EligibleVL ) AS EligibleVL12Mnths,
	SUM ( HasValidVL ) AS HasValidVL,
	SUM ( ValidVLSup ) AS VirallySuppressed,
	SUM (CASE WHEN _12MonthVL IS NOT NULL THEN 1 END  ) AS VLAt12Months,
	SUM ( [12MonthVLSup] ) AS VLAt12Months_Sup,
	SUM (CASE WHEN _18MonthVL IS NOT NULL THEN 1 END ) AS VLAt18Months,
	SUM ( [18MonthVLSup] ) AS VLAt18Months_Sup,
	SUM (CASE WHEN _24MonthVL IS NOT NULL THEN 1 END ) AS VLAt24Months,
	SUM ( [24MonthVLSup] ) AS VLAt24Months_Sup,
	SUM (CASE WHEN _6MonthVL IS NOT NULL THEN 1 END ) AS VLAt6Months,
	SUM ( [6MonthVLSup] ) AS VLAt6Months_Sup,
    CAST(GETDATE() AS DATE) AS LoadDate  
INTO [REPORTING].[dbo].AggregateVLUptakeOutcome
FROM NDWH.dbo.FactViralLoads vl
LEFT JOIN NDWH.dbo.DimAgeGroup g ON g.AgeGroupKey= vl.AgeGroupKey
LEFT JOIN NDWH.dbo.DimFacility f ON f.FacilityKey = vl.FacilityKey
LEFT JOIN NDWH.dbo.DimAgency a ON a.AgencyKey = vl.AgencyKey
LEFT JOIN NDWH.dbo.DimPatient pat ON pat.PatientKey = vl.PatientKey
LEFT JOIN NDWH.dbo.DimPartner p ON p.PartnerKey = vl.PartnerKey
LEFT JOIN NDWH.dbo.FactART art ON art.PatientKey = vl.PatientKey 
LEFT join NDWH.dbo.DimARTOutcome as outcome on outcome.ARTOutcomeKey = art.ARTOutcomeKey
LEFT JOIN NDWH.dbo.DimDate as date on date.DateKey = art.StartARTDateKey
WHERE IsTXCurr = 1 and outcome.ARTOutcome = 'V'
GROUP BY 
	MFLCode, 
	f.FacilityName, 
	County, 
	SubCounty, 
	p.PartnerName, 
	a.AgencyName,
	pat.Gender, 
	g.DATIMAgeGroup, 
	YEAR(art.StartARTDateKey),
    EOMONTH(date.Date),
	case 
		when vl.ValidVLResultCategory2 in  ('Low Risk LLV', 'LDL') then 'SUPPRESSED'
		else vl.ValidVLResultCategory2
	end,
	ValidVLResult