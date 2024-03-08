
GO
WITH HTS_DATASET AS (
    SELECT 
        DISTINCT
        MFLCode,
        f.FacilityName,
        County,
        SubCounty,
        p.PartnerName,
        a.AgencyName,
        Gender,
        age.DATIMAgeGroup AS AgeGroup,
        year,
        month,
        EOMONTH(d.Date) as AsOfDate,
        FORMAT(CAST(date AS date), 'MMMM') AS MonthName,
        SUM(Tested) AS Tested,
        SUM(Positive) AS Positive,
        SUM(Linked) AS Linked
    FROM NDWH.dbo.FactHTSClientTests hts
    LEFT JOIN NDWH.dbo.DimFacility f ON f.FacilityKey = hts.FacilityKey
    LEFT JOIN NDWH.dbo.DimAgency a ON a.AgencyKey = hts.AgencyKey
    LEFT JOIN NDWH.dbo.DimPatient pat ON pat.PatientKey = hts.PatientKey
    LEFT JOIN NDWH.dbo.DimAgeGroup age ON age.AgeGroupKey = hts.AgeGroupKey
    LEFT JOIN NDWH.dbo.DimPartner p ON p.PartnerKey = hts.PartnerKey
    LEFT JOIN NDWH.dbo.FactHTSClientLinkages link ON link.PatientKey = hts.PatientKey
    LEFT JOIN NDWH.dbo.DimDate d ON d.DateKey = hts.DateTestedKey
    WHERE TestType IN ('Initial Test', 'Initial')
    GROUP BY 
        MFLCode, 
        f.FacilityName,
        County, 
        SubCounty, 
        a.AgencyName, 
        p.PartnerName, 
        Gender, 
        age.DATIMAgeGroup,
        year, 
        month, 
        FORMAT(CAST(date AS date), 'MMMM'),
        EOMONTH(d.Date)
),
TXNEW_DATASET AS (
    SELECT 
        facility.MFLCode,
        facility.FacilityName,
        facility.SubCounty,
        facility.County,
        partner.PartnerName,
        agency.AgencyName,
        patient.Gender,
        age_group.DATIMAgeGroup,
        startDate.Year,
        startDate.Month,
        COUNT(*) AS TXNew
    FROM NDWH.dbo.FactArt AS art
    LEFT JOIN NDWH.dbo.DimFacility AS facility ON facility.FacilityKey = art.FacilityKey
    LEFT JOIN NDWH.dbo.DimPartner AS partner ON partner.PartnerKey = art.PartnerKey
    LEFT JOIN NDWH.dbo.DimPatient AS patient ON patient.PatientKey = art.PatientKey
    LEFT JOIN NDWH.dbo.DimAgeGroup AS age_group ON age_group.AgeGroupKey = art.AgeGroupKey
    LEFT JOIN NDWH.dbo.DimAgency AS agency ON agency.AgencyKey = art.AgencyKey    
    LEFT JOIN NDWH.dbo.DimDate AS startDate ON startDate.DateKey = art.StartARTDateKey
    GROUP BY 
        facility.MFLCode,
        facility.FacilityName,
        facility.SubCounty,
        facility.County,
        partner.PartnerName,
        agency.AgencyName,
        patient.Gender,
        age_group.DATIMAgeGroup,
        startDate.Year,
        startDate.Month 
)
SELECT 
    TXNEW_DATASET.*, 
    coalesce(Tested, 0) as Tested,
    coalesce(Positive, 0) as Positive,
    coalesce(Linked, 0) as Linked,
    CAST(GETDATE() AS DATE) AS LoadDate 
INTO REPORTING.dbo.AggregateTXNewHTSCascade
FROM TXNEW_DATASET
LEFT JOIN HTS_DATASET ON HTS_DATASET.MFLCode = TXNEW_DATASET.MFLCode
    AND HTS_DATASET.SubCounty=TXNEW_DATASET.SubCounty 
    AND HTS_DATASET.County=TXNEW_DATASET.County
    AND HTS_DATASET.PartnerName =TXNEW_DATASET.PartnerName 
    AND HTS_DATASET.AgencyName=TXNEW_DATASET.AgencyName
    AND HTS_DATASET.Gender=TXNEW_DATASET.Gender 
    AND HTS_DATASET.AgeGroup=TXNEW_DATASET.DATIMAgeGroup
    AND HTS_DATASET.Year=TXNEW_DATASET.Year 
    AND HTS_DATASET.Month=TXNEW_DATASET.Month
WHERE TXNEW_DATASET.MFLCode is not null --- to make sure we don't have orphaned facilties;