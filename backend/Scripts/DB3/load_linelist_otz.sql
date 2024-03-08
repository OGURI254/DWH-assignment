
GO

SELECT 
    PatientIDHash,
	PatientPKHash,
    NUPI,
	MFLCode,
	f.FacilityName,
	County,
	SubCounty,
	p.PartnerName,
	a.AgencyName,
	pat.Gender,
	age.DATIMAgeGroup as AgeGroup,
	otz.OTZEnrollmentDateKey,
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
	vl.ValidVLResultCategory2 as ValidVLResultCategory,
	vl.HasValidVL as HasValidVL,
	cast (art.StartARTDateKey as date) as startARTDate,
    CAST(GETDATE() AS DATE) AS LoadDate 
INTO [REPORTING].[dbo].[LineListOTZ]
FROM NDWH.dbo.FactOTZ otz
INNER join NDWH.dbo.DimAgeGroup age on age.AgeGroupKey=otz.AgeGroupKey
INNER join NDWH.dbo.DimFacility f on f.FacilityKey = otz.FacilityKey
INNER JOIN NDWH.dbo.DimAgency a on a.AgencyKey = otz.AgencyKey
INNER JOIN NDWH.dbo.DimPatient pat on pat.PatientKey = otz.PatientKey
INNER JOIN NDWH.dbo.DimPartner p on p.PartnerKey = otz.PartnerKey
LEFT JOIN NDWH.dbo.FactViralLoads vl on vl.PatientKey = otz.PatientKey and vl.PatientKey IS NOT NULL
LEFT JOIN NDWH.dbo.FACTART art on art.PatientKey = otz.PatientKey
WHERE age.Age BETWEEN 10 AND 19 AND IsTXCurr = 1

GO
