BEGIN

Select distinct 
    pat.PatientIDHash,
    pat.PatientPKHash,
    pat.Gender,
    pat.DOB,
    pat.MaritalStatus,
    pat.Nupi,
    pat.PatientSource,
    pat.ClientType,
    pat.SiteCode,
    fac.FacilityName,
    fac.County,
    fac.SubCounty,
    partner.PartnerName,
    agency.AgencyName,
    age.age,
    age.DATIMAgeGroup as AgeGroup,
    startdate.Date as StartARTDate,
    ART.CurrentRegimen,
    ART.CurrentRegimenline,
    ART.StartRegimen,
    ART.StartRegimenLine,
    ART.AgeAtEnrol,
    ART.AgeAtARTStart,
    ART.TimetoARTDiagnosis,
    ART.TimetoARTEnrollment,
    ART.PregnantARTStart,
    ART.PregnantAtEnrol,
    ART.LastVisitDate,
    ART.NextAppointmentDate,
    ART.StartARTAtThisfacility,
    ART.PreviousARTStartDate,
    ART.PreviousARTRegimen,
    case 
        when outcome.ARTOutcome is null then 'Others'
        else outcome.ARTOutcomeDescription
    end as ARTOutcomeDescription,
    vl.EligibleVL as Eligible4VL,
    vl.HasValidVL,
    vl.ValidVLSup,
    vl.LastVL,
    lastVL.Date as LastVLDate,
    vl.ValidVLResult,
    vl.ValidVLResultCategory1 as ValidVLResultCategory1,
    vl.ValidVLResultCategory2 as ValidVLResultCategory2,
    vl.HighViremia,
    vl.LowViremia,
    pat.ISTxCurr,
	dif.DifferentiatedCare,
    cast(getdate() as date) as LoadDate
INTO [REPORTING].[dbo].[Linelist_FACTART]
from  NDWH.dbo.FACTART As ART 
left join NDWH.dbo.DimPatient pat on pat.PatientKey = ART.PatientKey
left join NDWH.dbo.DimPartner partner on partner.PartnerKey = ART.PartnerKey
left join NDWH.dbo.DimAgency agency on agency.AgencyKey = ART.AgencyKey
left join NDWH.dbo.DimFacility fac on fac.FacilityKey = ART.FacilityKey
left join NDWH.dbo.DimAgeGroup age on age.AgeGroupKey = ART.AgeGroupKey
left join NDWH.dbo.DimDate startdate on startdate.DateKey = ART.StartARTDateKey
left join NDWH.dbo.DimARTOutcome as outcome on outcome.ARTOutcomeKey=ART.ARTOutcomeKey
left join NDWH.dbo.FactViralLoads as vl on vl.PatientKey = ART.PatientKey
left join NDWH.dbo.FactLatestObs as obs on obs.PatientKey = ART.PatientKey
left join NDWH.dbo.DimDifferentiatedCare as dif on dif.DifferentiatedCareKey = obs.DifferentiatedCareKey
left join NDWH.dbo.DimDate as lastVL on lastVL.DateKey =  vl.LastVLDateKey
WHERE ART.ARTOutcomeKey IS NOT NULL;
END
