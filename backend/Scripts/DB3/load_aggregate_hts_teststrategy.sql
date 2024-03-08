GO

SELECT
    MFLCode,
    f.FacilityName,
    County,
    SubCounty,
    p.PartnerName,
    a.AgencyName,
    Gender,
    age.DATIMAgeGroup as AgeGroup, 
    TestStrategy,
    d.year,
    d.month,
    FORMAT(cast(date as date), 'MMMM') MonthName,
    EOMONTH(d.date) as AsOfDate,
    Sum(Tested) as TestedClients,
    Sum(Positive) as PositiveClients,
    Sum(Linked) as LinkedClients,
     CAST(GETDATE() AS DATE) AS LoadDate 
INTO REPORTING.dbo.AggregateHTSTeststrategy
FROM NDWH.dbo.FactHTSClientTests hts
LEFT JOIN NDWH.dbo.DimFacility f on f.FacilityKey = hts.FacilityKey
LEFT JOIN NDWH.dbo.DimAgency a on a.AgencyKey = hts.AgencyKey
LEFT JOIN NDWH.dbo.DimPatient pat on pat.PatientKey = hts.PatientKey
LEFT JOIN NDWH.dbo.DimAgeGroup age on age.AgeGroupKey=hts.AgeGroupKey
LEFT JOIN NDWH.dbo.DimPartner p on p.PartnerKey = hts.PartnerKey
LEFT JOIN NDWH.dbo.FactHTSClientLinkages link on link.PatientKey = hts.PatientKey
LEFT JOIN NDWH.dbo.DimDate d on d.DateKey = hts.DateTestedKey
WHERE TestType in ('Initial Test', 'Initial')
GROUP BY 
    MFLCode, 
    f.FacilityName, 
    County, 
    SubCounty, 
    p.PartnerName, 
    a.AgencyName, 
    Gender, age.DATIMAgeGroup, 
    TestStrategy, 
    year, 
    month, 
    FORMAT(cast(date as date), 'MMMM'),
    EOMONTH(d.date)