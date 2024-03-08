GO

WITH prepCascade AS  (
    SELECT DISTINCT 
       PatientPKHash,
        MFLCode,        
        f.FacilityName,
        County,
        SubCounty,
        p.PartnerName,
        a.AgencyName,
        pat.Gender,
        age.DATIMAgeGroup as AgeGroup,
        ass.month AssMonth,
        ass.year AssYear,
        EOMONTH(ass.Date) as AsofDate,
        EligiblePrep,
        ScreenedPrep,
        ass.month As EnrollmentMonth, 
        ass.year As EnrollmentYear,
    CAST(GETDATE() AS DATE) AS LoadDate 
 
    FROM NDWH.dbo.FactPrepAssessments prep
    
    LEFT JOIN NDWH.dbo.DimFacility f on f.FacilityKey = prep.FacilityKey
    LEFT JOIN NDWH.dbo.DimAgency a on a.AgencyKey = prep.AgencyKey
    LEFT JOIN NDWH.dbo.DimPatient pat on pat.PatientKey = prep.PatientKey
    LEFT JOIN NDWH.dbo.DimAgeGroup age on age.AgeGroupKey=prep.AgeGroupKey
    LEFT JOIN NDWH.dbo.DimPartner p on p.PartnerKey = prep.PartnerKey
    LEFT JOIN NDWH.dbo.DimDate ass ON ass.DateKey = AssessmentVisitDateKey 
    
)
Select * 
  INTO REPORTING.dbo.LinelistPrep
  from prepCascade