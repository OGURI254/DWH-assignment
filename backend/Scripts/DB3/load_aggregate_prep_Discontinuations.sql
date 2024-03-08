GO

SELECT DISTINCT 
    MFLCode,		
    f.FacilityName,
    County,
    SubCounty,
    p.PartnerName,
    a.AgencyName,
    Gender,
    age.DATIMAgeGroup AS AgeGroup,
    d.Month AS ExitMonth,		
    d.Year AS ExitYear,
    EOMONTH(d.[Date]) as AsOfDate,
    ExitReason,
    COUNT(DISTINCT CONCAT(PrepNumber, PatientPKHash, MFLCode)) AS PrepDiscontinuations,
    CAST(GETDATE() AS DATE) AS LoadDate 
INTO REPORTING.dbo.AggregatePrepDiscontinuation
FROM NDWH.dbo.FactPrepDiscontinuation prep
LEFT JOIN NDWH.dbo.DimFacility f ON f.FacilityKey = prep.FacilityKey
LEFT JOIN NDWH.dbo.DimAgency a ON a.AgencyKey = prep.AgencyKey
LEFT JOIN NDWH.dbo.DimPatient pat ON pat.PatientKey = prep.PatientKey
LEFT JOIN NDWH.dbo.DimAgeGroup age ON age.AgeGroupKey = prep.AgeGroupKey
LEFT JOIN NDWH.dbo.DimPartner p ON p.PartnerKey = prep.PartnerKey
LEFT JOIN NDWH.dbo.DimDate d ON d.DateKey = prep.ExitdateKey
GROUP BY 
    MFLCode,
    f.FacilityName,
    County,
    SubCounty,
    p.PartnerName,
    a.AgencyName,
    Gender,
    age.DATIMAgeGroup,		
    d.Month,
    d.Year,
    EOMONTH(d.[Date]),
    ExitReason;
