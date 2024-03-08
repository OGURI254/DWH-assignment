SELECT DISTINCT
    MFLCode,
    f.FacilityName,
    County,
    SubCounty,
    p.PartnerName,
    a.AgencyName,
    Gender,
    age.DATIMAgeGroup as AgeGroup,
    CONVERT(char(7), cast(cast(OTZEnrollmentDateKey as char) as datetime), 23) as OTZEnrollmentYearMonth,
    EOMONTH(date.Date) as AsofDate,
    TransitionAttritionReason as Outcome,
    COUNT(TransitionAttritionReason) as patients_totalOutcome,
    CAST(GETDATE() AS DATE) AS LoadDate 
INTO REPORTING.dbo.AggregateOTZOutcome
FROM NDWH.dbo.FactOTZ otz
INNER join NDWH.dbo.DimAgeGroup age on age.AgeGroupKey=otz.AgeGroupKey
INNER join NDWH.dbo.DimFacility f on f.FacilityKey = otz.FacilityKey
INNER JOIN NDWH.dbo.DimAgency a on a.AgencyKey = otz.AgencyKey
INNER JOIN NDWH.dbo.DimPatient pat on pat.PatientKey = otz.PatientKey
INNER JOIN NDWH.dbo.DimPartner p on p.PartnerKey = otz.PartnerKey
LEFT JOIN NDWH.dbo.DimDate as date on date.DateKey = otz.OTZEnrollmentDateKey
WHERE TransitionAttritionReason is not null AND IsTXCurr = 1 AND age.Age BETWEEN 10 AND 19
GROUP BY 
    MFLCode, 
    f.FacilityName, 
    County, 
    SubCounty, 
    p.PartnerName, 
    a.AgencyName, 
    Gender, 
    age.DATIMAgeGroup, 
    CONVERT(char(7), cast(cast(OTZEnrollmentDateKey as char) as datetime), 23), 
    EOMONTH(date.Date),
    TransitionAttritionReason

