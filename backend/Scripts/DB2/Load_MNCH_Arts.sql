BEGIN
    --truncate table [ODS].[dbo].[MNCH_Arts]
	MERGE [ODS].[dbo].[MNCH_Arts] AS a
			USING(
					SELECT  distinct  P.[PatientPk],P.[SiteCode],P.[Emr], P.[Project], P.[Processed], P.[QueueId], P.[Status], P.[StatusDate], P.[DateExtracted]
						  , P.[Pkv], P.[PatientMnchID], P.[PatientHeiID], P.[FacilityName],[RegistrationAtCCC],[StartARTDate],[StartRegimen]
						  ,[StartRegimenLine],[StatusAtCCC],[LastARTDate],[LastRegimen],[LastRegimenLine], P.[Date_Created], P.[Date_Last_Modified]
					     ,[FacilityReceivingARTCare]
					   FROM [MNCHCentral].[dbo].[MnchArts] P(NoLock) 
				inner join (select tn.PatientPK,tn.SiteCode,max(tn.DateExtracted)MaxDateExtracted FROM [MNCHCentral].[dbo].[MnchArts] (NoLock)tn
				group by tn.PatientPK,tn.SiteCode)tm
					on P.PatientPk = tm.PatientPk and p.SiteCode = tm.SiteCode and p.DateExtracted = tm.MaxDateExtracted
			--  INNER JOIN  [MNCHCentral].[dbo].[MnchPatients] MnchP(Nolock) -- to be reviwed later
			--on P.patientPK = MnchP.patientPK and P.Sitecode = MnchP.Sitecode
			INNER JOIN [MNCHCentral].[dbo].[Facilities] F ON P.[FacilityId] = F.Id ) AS b 
						ON(
						 a.PatientPK  = b.PatientPK 
						and a.SiteCode = b.SiteCode
						and a.[PatientMnchID] = b.[PatientMnchID]
						
							)
					WHEN NOT MATCHED THEN 
						INSERT(PatientPk,SiteCode,Emr,Project,Processed,QueueId,[Status],StatusDate,DateExtracted,PatientMnchID,PatientHeiID,FacilityName,RegistrationAtCCC,StartARTDate,StartRegimen,StartRegimenLine,StatusAtCCC,LastARTDate,LastRegimen,LastRegimenLine,Date_Created,Date_Last_Modified,[FacilityReceivingARTCare],LoadDate)  
						VALUES(PatientPk,SiteCode,Emr,Project,Processed,QueueId,[Status],StatusDate,DateExtracted,PatientMnchID,PatientHeiID,FacilityName,RegistrationAtCCC,StartARTDate,StartRegimen,StartRegimenLine,StatusAtCCC,LastARTDate,LastRegimen,LastRegimenLine,Date_Created,Date_Last_Modified,[FacilityReceivingARTCare],Getdate())
				
					WHEN MATCHED THEN
						UPDATE SET 
							a.[Status]	 =b.[Status],
							a.LastRegimen = b.LastRegimen,
							a.StartRegimen = b.StartRegimen,
							a.StartRegimenLine = b.StartRegimenLine,
							a.[FacilityReceivingARTCare]  = b.[FacilityReceivingARTCare];
END





