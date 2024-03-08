
BEGIN
--truncate table [ODS].[dbo].[PrEP_Pharmacy]
MERGE [ODS].[dbo].[PrEP_Pharmacy] AS a
	USING(SELECT distinct
				   a.[Id]
				  ,a.[RefId]
				  ,a.[Created]
				  ,a.[PatientPk]
				  ,a.[SiteCode]
				  ,a.[Emr]
				  ,a.[Project]
				  ,a.[Processed]
				  ,a.[QueueId]
				  ,a.[Status]
				  ,a.[StatusDate]
				  ,a.[DateExtracted]
				  ,a.[FacilityId]
				  ,a.[FacilityName]
				  ,a.[PrepNumber]
				  ,a.[HtsNumber]
				  ,[VisitID]
				  ,[RegimenPrescribed]
				  ,[DispenseDate]
				  ,[Duration]
				  ,a.[Date_Created]
				  ,a.[Date_Last_Modified]

			  FROM [PREPCentral].[dbo].[PrepPharmacys](NoLock) a
				INNER JOIN (SELECT PatientPk, SiteCode, max(Created) AS maxCreated from [PREPCentral].[dbo].[PrepPharmacys]
							group by PatientPk,SiteCode) tn
					ON a.PatientPk = tn.PatientPk and a.SiteCode = tn.SiteCode and a.Created = tn.maxCreated

				INNER JOIN (SELECT PatientPk, SiteCode, max(DateExtracted) AS maxDateExtracted from [PREPCentral].[dbo].[PrepPharmacys]
							group by PatientPk,SiteCode) tm
					ON a.PatientPk = tm.PatientPk and a.SiteCode = tm.SiteCode and a.DateExtracted = tm.maxDateExtracted
				)	AS b 
	 
					ON(

						a.PatientPK  = b.PatientPK						
					and a.SiteCode = b.SiteCode
					and a.VisitID = b.VisitID
					and a.[DispenseDate] = b.[DispenseDate]
					) 

	 WHEN NOT MATCHED THEN 
		  INSERT(ID,RefId,Created,PatientPk,SiteCode,Emr,Project,Processed,QueueId,[Status],StatusDate,DateExtracted,FacilityId,FacilityName,PrepNumber,HtsNumber
		  ,VisitID,RegimenPrescribed,DispenseDate,Duration,Date_Created,Date_Last_Modified,LoadDate)
		  

		  VALUES(ID,RefId,Created,PatientPk,SiteCode,Emr,Project,Processed,QueueId,[Status],StatusDate,DateExtracted,FacilityId,FacilityName,PrepNumber,HtsNumber,
          VisitID,RegimenPrescribed,DispenseDate,Duration,Date_Created,Date_Last_Modified,Getdate())

	  WHEN MATCHED THEN
						UPDATE SET 														
							a.StatusDate=b.StatusDate,						
							a.RegimenPrescribed=b.RegimenPrescribed,
							a.Date_Last_Modified=b.Date_Last_Modified,
							a.Duration=b.Duration,
							a.EMR	=b.EMR;						
						

	END

					