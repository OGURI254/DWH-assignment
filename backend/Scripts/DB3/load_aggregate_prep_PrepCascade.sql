GO

WITH prepCascade AS  (
	SELECT DISTINCT 
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
        Sum(EligiblePrep) As EligiblePrep,
        sum(ScreenedPrep) As Screened,
        Count (distinct (concat(PrepNumber,PatientPKHash,MFLCode))) As PrepCT
	FROM NDWH.dbo.FactPrepAssessments prep
	LEFT JOIN NDWH.dbo.DimFacility f on f.FacilityKey = prep.FacilityKey
	LEFT JOIN NDWH.dbo.DimAgency a on a.AgencyKey = prep.AgencyKey
	LEFT JOIN NDWH.dbo.DimPatient pat on pat.PatientKey = prep.PatientKey
	LEFT JOIN NDWH.dbo.DimAgeGroup age on age.AgeGroupKey=prep.AgeGroupKey
	LEFT JOIN NDWH.dbo.DimPartner p on p.PartnerKey = prep.PartnerKey
	LEFT JOIN NDWH.dbo.DimDate ass ON ass.DateKey = AssessmentVisitDateKey 
	GROUP BY 
            MFLCode,
			f.FacilityName,
			County,
			SubCounty,
			p.PartnerName,
			a.AgencyName,
			pat.Gender,
			age.DATIMAgeGroup,
			ass.Month,
			ass.Year,
            EOMONTH(ass.Date)

),
prepStart AS (
	SELECT DISTINCT 
		MFLCode,		
		f.FacilityName,
		County,
		SubCounty,
		p.PartnerName,
		a.AgencyName,
		pat.Gender,
		age.DATIMAgeGroup as AgeGroup,
		enrol.month EnrollmentMonth, 
		enrol.year EnrollmentYear,
        EOMONTH(enrol.Date) as AsofDate,
		Count (distinct (concat(PrepNumber,PatientPKHash,MFLCode))) As StartedPrep
	FROM NDWH.dbo.FactPrepAssessments prep
	LEFT JOIN NDWH.dbo.DimFacility f on f.FacilityKey = prep.FacilityKey
	LEFT JOIN NDWH.dbo.DimAgency a on a.AgencyKey = prep.AgencyKey
	LEFT JOIN NDWH.dbo.DimPatient pat on pat.PatientKey = prep.PatientKey
	LEFT JOIN NDWH.dbo.DimAgeGroup age on age.AgeGroupKey=prep.AgeGroupKey
	LEFT JOIN NDWH.dbo.DimPartner p on p.PartnerKey = prep.PartnerKey
	LEFT JOIN NDWH.dbo.DimDate enrol ON enrol.DateKey = PrepEnrollmentDateKey	
	WHERE PrepEnrollmentDateKey IS NOT NULL
	GROUP BY MFLCode,
			f.FacilityName,
			County,
			SubCounty,
			p.PartnerName,
			a.AgencyName,
			pat.Gender,
			age.DATIMAgeGroup,
			enrol.Month,
			enrol.Year,
            EOMONTH(enrol.Date)
)	
SELECT
	COALESCE(p.MFLCode, s.MFLCode) AS MFLCode,		
	COALESCE(p.FacilityName, s.FacilityName) AS FacilityName,
	COALESCE(p.County, s.County) AS County,
	COALESCE(p.SubCounty, s.SubCounty) AS SubCounty,
	COALESCE(p.PartnerName, s.PartnerName) AS PartnerName,
	COALESCE(p.AgencyName, s.AgencyName) AS AgencyName,
	COALESCE(p.Gender, s.Gender) AS Gender,
	COALESCE(p.AgeGroup, s.AgeGroup) AS AgeGroup,
	COALESCE(p.AssMonth, s.EnrollmentMonth) AS AssMonth,
	COALESCE(p.AssYear, s.EnrollmentYear) AS AssYear,
    COALESCE(p.AsofDate, s.AsofDate) AS AsofDate,
	COALESCE(p.EligiblePrep, 0) AS EligiblePrep,
	COALESCE(p.Screened, 0) AS Screened,
	COALESCE(p.PrepCT, 0) AS PrepCT,
	COALESCE(s.StartedPrep, 0) AS StartedPrep,
    CAST(GETDATE() AS DATE) AS LoadDate 
  INTO REPORTING.dbo.AggregatePrepCascade
FROM prepCascade p
FULL OUTER JOIN prepStart s on p.MFLCode = s.MFLCode 
    and s.FacilityName = p.FacilityName 
    and s.County = p.County 
    and s.SubCounty = p.SubCounty 
    and s.PartnerName = p.PartnerName 
    and s.AgencyName = p.AgencyName 
    and s.Gender = p.Gender 
    and s.AgeGroup = s.AgeGroup 
    and AssMonth = EnrollmentMonth 
    and AssYear = EnrollmentYear
