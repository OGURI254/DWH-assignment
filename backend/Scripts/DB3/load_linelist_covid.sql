GO

SELECT 
	DISTINCT pat.PatientPKHash
	MFLCode,
	f.FacilityName,
	County,
	SubCounty,
	p.PartnerName,
	a.AgencyName,
	Gender,
	age.DATIMAgeGroup as AgeGroup,
	Covid19AssessmentDateKey,
	ReceivedCOVID19Vaccine,
	cast(cast(DateGivenFirstDoseKey as char) as date) as DateGivenFirstDoseKey,
	FirstDoseVaccineAdministered,
	cast(cast(DateGivenSecondDoseKey as char) as date) as DateGivenSecondDoseKey,
	SecondDoseVaccineAdministered,
	VaccinationStatus,
	VaccineVerification,
	BoosterGiven,
	BoosterDose,
	BoosterDoseDateKey,
	EverCOVID19Positive,
	COVID19TestDateKey,
	PatientStatus,
	AdmissionStatus,
	AdmissionUnit,
	MissedAppointmentDueToCOVID19,
	COVID19PositiveSinceLasVisit,
	COVID19TestDateSinceLastVisit,
	PatientStatusSinceLastVisit,
	AdmissionStatusSinceLastVisit,
	AdmissionStartDateKey,
	AdmissionEndDateKey,
	AdmissionUnitSinceLastVisit,
	SupplementalOxygenReceived,
	PatientVentilated,
	TracingFinalOutcome,
	CauseOfDeath,
	case when VaccinationStatus in ('Fully Vaccinated','Not Vaccinated','Partially Vaccinated') then 1 else 0 end as Screened,
	CAST(GETDATE() AS DATE) AS LoadDate 
INTO REPORTING.dbo.LineListCovid 
FROM NDWH.dbo.FactArt as art
LEFT JOIN NDWH.dbo.FactCovid as cov on cov.PatientKey = art.PatientKey
LEFT join NDWH.dbo.DimFacility f on f.FacilityKey = art.FacilityKey
LEFT JOIN NDWH.dbo.DimAgency a on a.AgencyKey = art.AgencyKey
LEFT JOIN NDWH.dbo.DimPatient pat on pat.PatientKey = art.PatientKey
LEFT join NDWH.dbo.DimAgeGroup age on age.AgeGroupKey = art.AgeGroupKey
LEFT JOIN NDWH.dbo.DimPartner p on p.PartnerKey = cov.PartnerKey
WHERE age.Age >= 12 AND pat.IsTXCurr = 1