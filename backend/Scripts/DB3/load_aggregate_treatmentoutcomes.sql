GO

SELECT DISTINCT
	MFLCode,
	f.FacilityName,
	County,
	SubCounty,
	p.PartnerName,
	a.AgencyName,
	pat.Gender,
	age.DATIMAgeGroup as AgeGroup,
	Year(StartARTDateKey) StartYear,
	Month(StartARTDateKey) StartMonth,
    EOMONTH(date.Date) AsOfDate,
	ARTOutcomeDescription,
	Count(ARTOutcomeDescription) TotalOutcomes,
    CAST(GETDATE() AS DATE) AS LoadDate 
INTO REPORTING.dbo.AggregateTreatmentOutcomes
FROM NDWH.dbo.FACTART art
INNER join NDWH.dbo.DimAgeGroup age on age.AgeGroupKey= art.AgeGroupKey
INNER join NDWH.dbo.DimFacility f on f.FacilityKey = art.FacilityKey
INNER JOIN NDWH.dbo.DimAgency a on a.AgencyKey = art.AgencyKey
INNER JOIN NDWH.dbo.DimPatient pat on pat.PatientKey = art.PatientKey
INNER JOIN NDWH.dbo.DimPartner p on p.PartnerKey = art.PartnerKey
INNER JOIN NDWH.dbo.DimARTOutcome ot on ot.ARTOutcomeKey = art.ARTOutcomeKey
INNER JOIN NDWH.dbo.DimDate as date on date.DateKey = art.StartARTDateKey
GROUP BY 
    MFLCode, 
    f.FacilityName,
    County,
    SubCounty, 
    p.PartnerName, 
    a.AgencyName, 
    pat.Gender, 
    age.DATIMAgeGroup, 
    Year(StartARTDateKey) ,
    Month(StartARTDateKey),
    EOMONTH(date.Date),
    ARTOutcomeDescription