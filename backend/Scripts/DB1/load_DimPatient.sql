
	IF OBJECT_ID(N'[NDWH].[dbo].[DimPatient]', N'U') IS NOT NULL 
	DROP TABLE [NDWH].[dbo].[DimPatient];
BEGIN

    with ct_patient_source as (
        select
            distinct
            patients.PatientIDHash,
            patients.PatientPKHash,
            patients.PatientID,
            patients.PatientPK,
            patients.SiteCode,
            Gender,
            cast(DOB as date) as DOB,
            MaritalStatus,
            NupiHash,
            PatientType,
            PatientSource,
            baselines.eWHO as EnrollmentWHOKey,
            cast(format(coalesce(eWHODate, '1900-01-01'),'yyyyMMdd') as int) as DateEnrollmentWHOKey,
            bWHO as BaseLineWHOKey,
            cast(format(coalesce(bWHODate, '1900-01-01'),'yyyyMMdd') as int) as DateBaselineWHOKey,
            case 
                when outcomes.ARTOutcome =  'V' then 1
                else 0
            end as IsTXCurr,
            cast(getdate() as date) as LoadDate
        from 
        ODS.dbo.CT_Patient as patients
        left join ODS.dbo.CT_PatientBaselines as baselines on patients.PatientPKHash = baselines.PatientPKHash 
            and patients.SiteCode = baselines.SiteCode
        left join ODS.dbo.Intermediate_ARTOutcomes as outcomes on outcomes.PatientPKHash = patients.PatientPKHash 
            and outcomes.SiteCode = patients.SiteCode
	),
    hts_patient_source as (
        select    
            distinct HTSNumberHash,
            PatientPKHash,
            PatientPK,
            SiteCode,
            cast(DOB as date) as DOB,
            Gender,
            MaritalStatus,
            NupiHash
        from ODS.dbo.HTS_clients as clients
    ),
    prep_patient_source as (
    select 
        distinct PatientPkHash,
        PatientPk,
        PrepNumber,
        SiteCode,
        PrepEnrollmentDate,
        Sex,
        DateofBirth,
        ClientType,
        MaritalStatus
    from ODS.dbo.PrEP_Patient
    ),
    combined_data_ct_hts as (
        select
            coalesce(ct_patient_source.PatientPKHash, hts_patient_source.PatientPKHash) as PatientPKHash,
            --coalesce(ct_patient_source.PatientPKHash, hts_patient_source.PatientPKHash) as PatientPKHash,
            coalesce(ct_patient_source.SiteCode, hts_patient_source.SiteCode) as SiteCode,
            coalesce(ct_patient_source.NupiHash, hts_patient_source.NupiHash) as NUPI,
            coalesce(ct_patient_source.DOB, hts_patient_source.DOB) as DOB,
            coalesce(ct_patient_source.MaritalStatus, hts_patient_source.MaritalStatus) as MaritalStatus,
            coalesce(ct_patient_source.Gender, hts_patient_source.Gender) as Gender,
            ct_patient_source.PatientIDHash,
            ct_patient_source.PatientType as ClientType,
			ct_patient_source.PatientSource,
			ct_patient_source.EnrollmentWHOKey,
			ct_patient_source.DateEnrollmentWHOKey,
			ct_patient_source.BaseLineWHOKey,
			ct_patient_source.DateBaselineWHOKey,
			ct_patient_source.IsTXCurr,
            hts_patient_source.HTSNumberHash,
			cast(getdate() as date) as LoadDate
        from ct_patient_source 
        full join hts_patient_source on  hts_patient_source.PatientPKHash = ct_patient_source.PatientPKHash
            and ct_patient_source.SiteCode = hts_patient_source.SiteCode
    ),
    combined_data_ct_hts_prep as (
        select
            coalesce(combined_data_ct_hts.PatientPKHash, prep_patient_source.PatientPKHash) as PatientPKHash,
            coalesce(combined_data_ct_hts.SiteCode, prep_patient_source.SiteCode) as SiteCode,
            combined_data_ct_hts.NUPI as NUPI,
            coalesce(combined_data_ct_hts.DOB, prep_patient_source.DateofBirth) as DOB,
            coalesce(combined_data_ct_hts.MaritalStatus, prep_patient_source.MaritalStatus) as MaritalStatus,
            coalesce(combined_data_ct_hts.Gender, prep_patient_source.Sex) as Gender,
            combined_data_ct_hts.PatientIDHash,
            coalesce(combined_data_ct_hts.ClientType, prep_patient_source.ClientType) as ClientType,
			combined_data_ct_hts.PatientSource,
			combined_data_ct_hts.EnrollmentWHOKey,
			combined_data_ct_hts.DateEnrollmentWHOKey,
			combined_data_ct_hts.BaseLineWHOKey,
			combined_data_ct_hts.DateBaselineWHOKey,
			combined_data_ct_hts.IsTXCurr,
            combined_data_ct_hts.HTSNumberHash,
            prep_patient_source.PrepNumber,
            cast(format(prep_patient_source.PrepEnrollmentDate,'yyyyMMdd') as int) as PrepEnrollmentDateKey,
			cast(getdate() as date) as LoadDate
        from combined_data_ct_hts 
        full join prep_patient_source on combined_data_ct_hts.PatientPKHash = prep_patient_source.PatientPKHash
            and prep_patient_source.SiteCode = combined_data_ct_hts.SiteCode            
    )
	select
        PatientKey = IDENTITY(INT, 1, 1),
        combined_data_ct_hts_prep.PatientIDHash,
		combined_data_ct_hts_prep.PatientPKHash,
        combined_data_ct_hts_prep.HtsNumberHash,
        combined_data_ct_hts_prep.PrepNumber,
        combined_data_ct_hts_prep.SiteCode,
        combined_data_ct_hts_prep.NUPI,
        combined_data_ct_hts_prep.DOB,
        combined_data_ct_hts_prep.MaritalStatus,
        CASE 
            WHEN combined_data_ct_hts_prep.Gender = 'M' THEN 'Male'
            WHEN combined_data_ct_hts_prep.Gender = 'F' THEN 'Female'
            ELSE combined_data_ct_hts_prep.Gender 
        END AS Gender,
        combined_data_ct_hts_prep.ClientType,
        combined_data_ct_hts_prep.PatientSource,
        combined_data_ct_hts_prep.EnrollmentWHOKey,
        combined_data_ct_hts_prep.DateBaselineWHOKey,
        combined_data_ct_hts_prep.BaseLineWHOKey,
        combined_data_ct_hts_prep.PrepEnrollmentDateKey,
        combined_data_ct_hts_prep.IsTXCurr,
        combined_data_ct_hts_prep.LoadDate
	into NDWH.dbo.DimPatient
	from combined_data_ct_hts_prep

ALTER TABLE NDWH.dbo.DimPatient ADD PRIMARY KEY(PatientKey);

END
