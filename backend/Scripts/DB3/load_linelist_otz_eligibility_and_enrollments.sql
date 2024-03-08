IF OBJECT_ID(N'[REPORTING].[dbo].[LineListOTZEligibilityAndEnrollments]', N'U') IS NOT NULL 			
	DROP TABLE [REPORTING].[dbo].[LineListOTZEligibilityAndEnrollments]
GO
--- A linelist of ALHIV patients (Enrolled + Not Enrolled to OTZ)
SELECT DISTINCT
	PatientPKHash,
    PatientIDHash,
	MFLCode,
	f.FacilityName,
	County,
	SubCounty,
	p.PartnerName,
	a.AgencyName,
	pat.Gender,
	age.DATIMAgeGroup as AgeGroup,
	date.Date as OTZEnrollmentDate,
	LastVisitDateKey,
	TransitionAttritionReason,
	TransferInStatus,
	case when otz.ModulesPreviouslyCovered is not null then 1 else 0 end as CompletedTraining,
	ModulesPreviouslyCovered,
	ModulesCompletedToday_OTZ_Orientation,
	ModulesCompletedToday_OTZ_Participation,
	ModulesCompletedToday_OTZ_Leadership,
	ModulesCompletedToday_OTZ_MakingDecisions,
	ModulesCompletedToday_OTZ_Transition,
	ModulesCompletedToday_OTZ_TreatmentLiteracy,
	ModulesCompletedToday_OTZ_SRH,
	ModulesCompletedToday_OTZ_Beyond,
	FirstVL,
	LastVL,
	vl.EligibleVL,
	ValidVLResult,
	ValidVLResultCategory1,
	ValidVLResultCategory2,
	HasValidVL,
	COUNT(CASE WHEN art.PatientKey is not null THEN 1 ELSE 0 END) as Eligible,
	COUNT(CASE WHEN otz.PatientKey is not null THEN 1 ELSE NULL END) as Enrolled,
    CAST(GETDATE() AS DATE) AS LoadDate 
INTO [REPORTING].[dbo].[LineListOTZEligibilityAndEnrollments]
FROM NDWH.dbo.FACTART art
INNER JOIN NDWH.dbo.DimAgeGroup age ON age.AgeGroupKey= art.AgeGroupKey
INNER JOIN NDWH.dbo.DimFacility f ON f.FacilityKey = art.FacilityKey
INNER JOIN NDWH.dbo.DimAgency a ON a.AgencyKey = art.AgencyKey
INNER JOIN NDWH.dbo.DimPatient pat ON pat.PatientKey = art.PatientKey
INNER JOIN NDWH.dbo.DimPartner p ON p.PartnerKey = art.PartnerKey
LEFT JOIN NDWH.dbo.FactViralLoads vl ON vl.PatientKey = art.PatientKey AND vl.PatientKey IS NOT NULL 
FULL OUTER JOIN NDWH.dbo.FactOTZ otz on otz.PatientKey = art.PatientKey
LEFT JOIN NDWH.dbo.DimDate as date on date.DateKey = otz.OTZEnrollmentDateKey
WHERE age.Age BETWEEN 10 AND 19  AND IsTXCurr = 1
GROUP BY 
	PatientPKHash, 
    PatientIDHash,
    MFLCode,
	f.FacilityName,
	County,
	SubCounty,
	p.PartnerName,
	a.AgencyName,
	pat.Gender,
	age.DATIMAgeGroup,
	date.Date,
	LastVisitDateKey,
	TransitionAttritionReason,
	TransferInStatus,
	case when otz.ModulesPreviouslyCovered is not null then 1 else 0 end,
	ModulesPreviouslyCovered,
	ModulesCompletedToday_OTZ_Orientation,
	ModulesCompletedToday_OTZ_Participation,
	ModulesCompletedToday_OTZ_Leadership,
	ModulesCompletedToday_OTZ_MakingDecisions,
	ModulesCompletedToday_OTZ_Transition,
	ModulesCompletedToday_OTZ_TreatmentLiteracy,
	ModulesCompletedToday_OTZ_SRH,
	ModulesCompletedToday_OTZ_Beyond,
	FirstVL,
	LastVL,
	EligibleVL,
	ValidVLResult,
	ValidVLResultCategory1,
	ValidVLResultCategory2,
	HasValidVL
GO