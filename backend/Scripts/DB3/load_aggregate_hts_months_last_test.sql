GO

SELECT DISTINCT
    MFLCode,
    f.FacilityName,
    County,
    SubCounty,
    p.PartnerName,
    a.AgencyName,
    Gender,
    age.DATIMAgeGroup as AgeGroup,
    MonthsLastTest MonthLastTest,
    year,
    month,
    EOMONTH(cast(concat([year], '-', [month], '-01') as date)) as AsOfDate,
    FORMAT(cast(date as date), 'MMMM') MonthName,
    Sum(Tested) Tested,
    Sum(Positive) Positive,
    Sum(Linked) Linked,
    CAST(GETDATE() AS DATE) AS LoadDate
INTO REPORTING.dbo.AggregateHTSMonthsLastTest 
FROM NDWH.dbo.FactHTSClientTests hts
LEFT join NDWH.dbo.DimFacility f on f.FacilityKey = hts.FacilityKey
LEFT JOIN NDWH.dbo.DimAgency a on a.AgencyKey = hts.AgencyKey
LEFT JOIN NDWH.dbo.DimPatient pat on pat.PatientKey = hts.PatientKey
LEFT join NDWH.dbo.DimAgeGroup age on age.AgeGroupKey=hts.AgeGroupKey
LEFT JOIN NDWH.dbo.DimPartner p on p.PartnerKey = hts.PartnerKey
LEFT JOIN NDWH.dbo.FactHTSClientLinkages link on link.PatientKey = hts.PatientKey
LEFT JOIN NDWH.dbo.DimDate d on d.DateKey = hts.DateTestedKey
where TestType in ('Initial test','Initial')
GROUP BY 
    MFLCode, 
    f.FacilityName,
    County, 
    SubCounty, 
    p.PartnerName, 
    a.AgencyName, 
    Gender, 
    age.DATIMAgeGroup, 
    MonthsLastTest, 
    year,
    month, 
    EOMONTH(cast(concat([year], '-', [month], '-01') as date)),
    FORMAT(cast(date as date), 'MMMM')