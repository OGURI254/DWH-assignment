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
	pat.PatientPKHash,
	pat.PatientIDHash,
	LatestVL1,
	vl.LatestVLDate1Key,
	LatestVL2,
	vl.LatestVLDate2Key,
	LatestVL3,
	vl.LatestVLDate3Key,
	PBFValidVL,
    CAST(GETDATE() AS DATE) AS LoadDate 
INTO REPORTING.dbo.LineListViralLoad
FROM NDWH.dbo.FactViralLoads vl
INNER join NDWH.dbo.DimAgeGroup age on age.AgeGroupKey=vl.AgeGroupKey
INNER join NDWH.dbo.DimFacility f on f.FacilityKey = vl.FacilityKey
INNER JOIN NDWH.dbo.DimAgency a on a.AgencyKey = vl.AgencyKey
INNER JOIN NDWH.dbo.DimPatient pat on pat.PatientKey = vl.PatientKey
INNER JOIN NDWH.dbo.DimPartner p on p.PartnerKey = vl.PartnerKey

GO
