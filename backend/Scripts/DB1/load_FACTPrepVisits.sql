IF OBJECT_ID(N'[NDWH].[dbo].[FactPrepVisits]', N'U') IS NOT NULL 
DROP TABLE [NDWH].[dbo].[FactPrepVisits];

BEGIN

    with MFL_partner_agency_combination as (
        select 
            distinct MFL_Code,
            SDP,
        SDP_Agency  as Agency
        from ODS.dbo.All_EMRSites 
    ),
    prep_patients as
    (
        select
            distinct convert(nvarchar(64), hashbytes('SHA2_256', cast(PatientPK as nvarchar(36))), 2) as PatientPK,
            SiteCode
        from ODS.dbo.PrEP_Patient
        where ODS.dbo.PrEP_Patient.PrepNumber is not null
    ),

PrepVisits as (
        select 
            convert(nvarchar(64), hashbytes('SHA2_256', cast(PatientPK as nvarchar(36))), 2) as PatientPK,
            SiteCode,    
            VisitID,
            VisitDate,
            BloodPressure,
            Temperature,
            Weight,
            Height,
            BMI,
            STIScreening,
            STISymptoms,
            STITreated,
            Circumcised,
            VMMCReferral,
            LMP,
            MenopausalStatus,
            PregnantAtThisVisit,
            EDD,
            PlanningToGetPregnant,
            PregnancyPlanned,
            PregnancyEnded,
            PregnancyEndDate,
            PregnancyOutcome,
            BirthDefects,
            Breastfeeding,
            FamilyPlanningStatus,
            FPMethods,
            AdherenceDone,
            AdherenceOutcome,
            AdherenceReasons,
            SymptomsAcuteHIV,
            ContraindicationsPrep,
            PrepTreatmentPlan,
            PrepPrescribed,
            RegimenPrescribed,
            MonthsPrescribed,
            CondomsIssued,
            Tobegivennextappointment,
            Reasonfornotgivingnextappointment,
            HepatitisBPositiveResult,
            HepatitisCPositiveResult,
            VaccinationForHepBStarted,
            TreatedForHepB,
            VaccinationForHepCStarted,
            TreatedForHepC,
            NextAppointment,
            ClinicalNotes
        from ODS.DBO.PrEP_Visits
        where VisitDate is not null

    )

    select 
        FactKey = IDENTITY(INT, 1, 1),
        patient.PatientKey,
        facility.FacilityKey,
        agency.AgencyKey,
        partner.PartnerKey,
        age_group.AgeGroupKey,
        visit.DateKey as VisitDateKey,
        appointment.DateKey as NextAppointmentDateKey,
        pregnancy.DateKey as PregnancyEndDateKey,
        PrepVisits.VisitID,
        PrepVisits.BloodPressure,
        PrepVisits.Temperature,
        PrepVisits.Weight,
        PrepVisits.Height,
        PrepVisits.BMI,
        PrepVisits.STIScreening,
        PrepVisits.STISymptoms,
        case when   PrepVisits.STISymptoms  is not null then 1 else 0 end as STIPositive,
        case when   PrepVisits.STISymptoms  is  null then 1 else 0 end as STINegative,
        PrepVisits.STITreated,
        PrepVisits.Circumcised,
        PrepVisits.VMMCReferral,
        PrepVisits.LMP,
        PrepVisits.MenopausalStatus,
        PrepVisits.PregnantAtThisVisit,
        PrepVisits.EDD,
        PrepVisits.PlanningToGetPregnant,
        PrepVisits.PregnancyPlanned,
        PrepVisits.PregnancyEnded,
        PrepVisits.PregnancyEndDate,
        PrepVisits.PregnancyOutcome,
        PrepVisits.BirthDefects,
        PrepVisits.Breastfeeding,
        PrepVisits.FamilyPlanningStatus,
        PrepVisits.FPMethods,
        PrepVisits.AdherenceDone,
        PrepVisits.AdherenceOutcome,
        PrepVisits.AdherenceReasons,
        PrepVisits.SymptomsAcuteHIV,
        PrepVisits.ContraindicationsPrep,
        PrepVisits.PrepTreatmentPlan,
        PrepVisits.PrepPrescribed,
        PrepVisits.RegimenPrescribed,
        PrepVisits.MonthsPrescribed,
        PrepVisits.CondomsIssued,
        PrepVisits.Tobegivennextappointment,
        PrepVisits.Reasonfornotgivingnextappointment,
        PrepVisits.HepatitisBPositiveResult,
        PrepVisits.HepatitisCPositiveResult,
        PrepVisits.VaccinationForHepBStarted,
        PrepVisits.TreatedForHepB,
        PrepVisits.VaccinationForHepCStarted,
        PrepVisits.TreatedForHepC,
        PrepVisits.NextAppointment,
        PrepVisits.ClinicalNotes,
        cast(getdate() as date) as LoadDate
    into NDWH.dbo.FactPrepVisits
    from prep_patients
    left join PrepVisits as  PrepVisits on PrepVisits.PatientPK = prep_patients.PatientPK
        and PrepVisits.SiteCode = prep_patients.SiteCode
    left join NDWH.dbo.DimPatient as patient on patient.PatientPKHash = prep_patients.PatientPK
        and patient.SiteCode = prep_patients.SiteCode
    left join MFL_partner_agency_combination on MFL_partner_agency_combination.MFL_Code = prep_patients.SiteCode
    left join NDWH.dbo.DimPartner as partner on partner.PartnerName = MFL_partner_agency_combination.SDP 
    left join NDWH.dbo.DimAgency as agency on agency.AgencyName = MFL_partner_agency_combination.Agency
    left join NDWH.dbo.DimFacility as facility on facility.MFLCode = prep_patients.SiteCode
    left join NDWH.dbo.DimAgeGroup as age_group on age_group.Age = datediff(yy, patient.DOB, coalesce(PrepVisits.VisitDate, getdate()))
    left join NDWH.dbo.DimDate as visit on visit.Date = PrepVisits.VisitDate
    left join NDWH.dbo.DimDate as pregnancy on pregnancy.Date = PrepVisits.PregnancyEndDate
    left join NDWH.dbo.DimDate as appointment on appointment.Date= PrepVisits.NextAppointment;
    
    alter table NDWH.dbo.FactPrepVisits add primary key(FactKey);

END

