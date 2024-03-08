SELECT 
    MFLCode,
    FacilityName,
    County,
    SubCounty,
    PartnerName,
    AgencyName,
    Gender,
    AgeGroup,
    clientTestedAs,
    [year],
    [month],
    MonthName,
    AsofDate,
    Tested,
    Linked,
    Positive,
    CAST(GETDATE() AS DATE) AS LoadDate
INTO REPORTING.dbo.AggregateClientTestedAs
FROM (
    SELECT DISTINCT
        MFLCode,
        f.FacilityName,
        County,
        SubCounty,
        p.PartnerName,
        a.AgencyName,
        Gender,
        age.DATIMAgeGroup AS AgeGroup,
        clientTestedAs,
        d.[year],
        d.[month],
        DATENAME(month, d.Date) AS MonthName,
        EOMONTH(d.date) AS AsofDate,
        SUM(Tested) AS Tested,
        SUM(Linked) AS Linked,
        SUM(Positive) AS Positive,
        CAST(GETDATE() AS DATE) AS LoadDate
    FROM NDWH.dbo.FactHTSClientTests hts
    LEFT JOIN NDWH.dbo.DimFacility f ON f.FacilityKey = hts.FacilityKey
    LEFT JOIN NDWH.dbo.DimAgency a ON a.AgencyKey = hts.AgencyKey
    LEFT JOIN NDWH.dbo.DimPatient pat ON pat.PatientKey = hts.PatientKey
    LEFT JOIN NDWH.dbo.DimAgeGroup age ON age.AgeGroupKey = hts.AgeGroupKey
    LEFT JOIN NDWH.dbo.DimPartner p ON p.PartnerKey = hts.PartnerKey
    LEFT JOIN NDWH.dbo.DimDate d ON d.DateKey = hts.DateTestedKey
    WHERE TestType IN ('Initial test', 'Initial')
    GROUP BY
        MFLCode,
        f.FacilityName,
        County,
        SubCounty,
        p.PartnerName,
        a.AgencyName,
        Gender,
        age.DATIMAgeGroup,
        clientTestedAs,
        d.[year],
        d.[month],
        DATENAME(month, d.Date),
        EOMONTH(d.date)

) as tbl;