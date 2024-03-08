
IF OBJECT_ID(N'[NDWH].[dbo].[FACTART]', N'U') IS NOT NULL 
	DROP TABLE [NDWH].[dbo].[FACTART];
BEGIN
with MFL_partner_agency_combination as (
	select 
		distinct MFL_Code,
		SDP,
	  SDP_Agency as Agency
	from ODS.dbo.All_EMRSites 
),

   Patient As ( Select    
     
      Patient.PatientIDHash,
      Patient.PatientPKHash,
      cast (Patient.SiteCode as nvarchar) As SiteCode,
      DATEDIFF(yy,Patient.DOB,Patient.RegistrationAtCCC) AgeAtEnrol,
      DATEDIFF(yy,Patient.DOB,ART.StartARTDate) AgeAtARTStart,
      ART.StartARTAtThisfacility,
      ART.PreviousARTStartDate,
      ART.PreviousARTRegimen,
      ART.StartARTDate,
      LastARTDate,
      CASE WHEN [DateConfirmedHIVPositive] IS NOT NULL AND ART.StartARTDate IS NOT NULL
				 THEN CASE WHEN DateConfirmedHIVPositive<= ART.StartARTDate THEN DATEDIFF(DAY,DateConfirmedHIVPositive,ART.StartARTDate)
					ELSE NULL END
				ELSE NULL END AS TimetoARTDiagnosis,
      CASE WHEN Patient.RegistrationAtCCC IS NOT NULL AND ART.StartARTDate IS NOT NULL
				THEN CASE WHEN Patient.RegistrationAtCCC<=ART.StartARTDate  THEN DATEDIFF(DAY,Patient.[RegistrationAtCCC],ART.StartARTDate)
				ELSE NULL END
				ELSE NULL END AS TimetoARTEnrollment,
        Pre.PregnantARTStart,
        Pre.PregnantAtEnrol,
        las.LastEncounterDate As LastVisitDate,
        las.NextAppointmentDate,
        datediff(yy, patient.DOB, las.LastEncounterDate) as AgeLastVisit,
        lastRegimen,
        StartRegimen,
        lastRegimenline,
        StartRegimenline,
        outcome.ARTOutcome

from 
ODS.dbo.CT_Patient Patient
inner join ODS.dbo.CT_ARTPatients ART on ART.PatientPK=Patient.Patientpk and ART.SiteCode=Patient.SiteCode
left join ODS.dbo.Intermediate_PregnancyAsATInitiation   Pre on Pre.Patientpk= Patient.PatientPK and Pre.SiteCode=Patient.SiteCode
left join ODS.dbo.Intermediate_LastPatientEncounter las on las.PatientPK =Patient.PatientPK  and las.SiteCode =Patient.SiteCode 
left join ODS.dbo.Intermediate_ARTOutcomes  outcome on outcome.PatientPK=Patient.PatientPK and outcome.SiteCode=Patient.SiteCode
   )

   Select 
            Factkey = IDENTITY(INT, 1, 1),
            pat.PatientKey,
            fac.FacilityKey,
            partner.PartnerKey,
            agency.AgencyKey,
            age_group.AgeGroupKey,
            StartARTDate.DateKey As StartARTDateKey,
            LastARTDate.DateKey  as LastARTDateKey,
            ARTOutcome.ARTOutcomeKey,
            lastRegimen As CurrentRegimen,
            LastRegimenLine As CurrentRegimenline,
            StartRegimen,
            StartRegimenLine,
            AgeAtEnrol,
            AgeAtARTStart,
            AgeLastVisit,
            CASE
              WHEN floor( AgeLastVisit ) < 15 THEN
              'Child' 
              WHEN floor( AgeLastVisit ) >= 15 THEN
              'Adult' ELSE 'Aii' 
            END AS Agegrouping,
            TimetoARTDiagnosis,
            TimetoARTEnrollment,
            PregnantARTStart,
            PregnantAtEnrol,
            LastVisitDate,
            Patient.NextAppointmentDate,
            StartARTAtThisfacility,
            PreviousARTStartDate,
            PreviousARTRegimen,
            cast(getdate() as date) as LoadDate
INTO NDWH.dbo.FACTART
from  Patient
left join NDWH.dbo.DimPatient as Pat on pat.PatientPKHash=Patient.PatientPkHash and Pat.SiteCode=Patient.SiteCode
left join NDWH.dbo.Dimfacility fac on fac.MFLCode=Patient.SiteCode
left join MFL_partner_agency_combination on MFL_partner_agency_combination.MFL_Code  = Patient.SiteCode 
left join NDWH.dbo.DimPartner as partner on partner.PartnerName = MFL_partner_agency_combination.SDP 
left join NDWH.dbo.DimAgeGroup as age_group on age_group.Age = Patient.AgeLastVisit
left join NDWH.dbo.DimDate as StartARTDate on StartARTDate.Date = Patient.StartARTDate
left join NDWH.dbo.DimDate as LastARTDate on  LastARTDate.Date=Patient.LastARTDate
left join NDWH.dbo.DimAgency as agency on agency.AgencyName = MFL_partner_agency_combination.Agency
left join ODS.dbo.Intermediate_ARTOutcomes As IOutcomes  on IOutcomes.PatientPKHash = Patient.PatientPkHash  and IOutcomes.SiteCode = Patient.SiteCode
left join NDWH.dbo.DimARTOutcome ARTOutcome on ARTOutcome.ARTOutcome=IOutcomes.ARTOutcome;

alter table NDWH.dbo.FactART add primary key(FactKey)

END






