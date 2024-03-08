GO

WITH tested AS (
    SELECT distinct 
        MFLCode,
        FacilityName,
        hts.PatientKey,
        County,
        SubCounty,
        PartnerName,
        AgencyName,
        Gender,
        DATIMAgeGroup,
        tbScreening,
        case 
            when TBScreening is not null then 'Screened for TB'
            else 'Not Screened for TB' 
        end as TBScreening_Grp,
        d.year,
        d.month,
        FORMAT(cast(date as date), 'MMMM') MonthName,  
        EOMONTH(d.[Date]) as AsOfDate,  
        Tested,        
        Positive,         
        Linked
    FROM NDWH.dbo.FactHTSClientTests hts
    LEFT join NDWH.dbo.DimFacility f on f.FacilityKey = hts.FacilityKey
    LEFT JOIN NDWH.dbo.DimAgency a on a.AgencyKey = hts.AgencyKey
    LEFT JOIN NDWH.dbo.DimPatient pat on pat.PatientKey = hts.PatientKey
    LEFT join NDWH.dbo.DimAgeGroup age on age.AgeGroupKey=hts.AgeGroupKey
    LEFT JOIN NDWH.dbo.DimPartner p on p.PartnerKey = hts.PartnerKey
    LEFT JOIN NDWH.dbo.FactHTSClientLinkages link on link.PatientKey = hts.PatientKey
    LEFT JOIN NDWH.dbo.DimDate d on d.DateKey = hts.DateTestedKey
    WHERE TestType in ('Initial Test', 'Initial')
)
SELECT 
    MFLCode,
    FacilityName,
    County,
    SubCounty,
    PartnerName,
    AgencyName,
    Gender,
    DATIMAgeGroup as AgeGroup,
    tbScreening,
    TBScreening_Grp,
    year,
    month,
    MonthName,
    AsOfDate,
    Sum(Tested) Tested,
    Sum(Positive) Positive,
    Sum(Linked) Linked,
    CAST(GETDATE() AS DATE) AS LoadDate 
    INTO REPORTING.dbo.AggregateHTSTBscreening
FROM tested
GROUP BY 
    MFLCode, 
    FacilityName, 
    County,
    SubCounty, 
    PartnerName, 
    AgencyName, 
    Gender, 
    DATIMAgeGroup, 
    tbScreening, 
    year, 
    month, 
    MonthName, 
    AsOfDate,
    TBScreening_Grp