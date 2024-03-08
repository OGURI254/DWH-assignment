BEGIN

		--Truncate table [ODS].[dbo].[HTS_ClientTracing]
		MERGE [ODS].[dbo].[HTS_ClientTracing] AS a
			USING(SELECT DISTINCT  a.[FacilityName]
				  ,a.[SiteCode]
				  ,a.[PatientPk]
				  ,a.[HtsNumber]
				  ,a.[Emr]
				  ,a.[Project]     
				  ,[TracingType]
				  ,[TracingDate]
				  ,[TracingOutcome]
			  FROM [HTSCentral].[dbo].[HtsClientTracing] (NoLock)a
				INNER JOIN [HTSCentral].[dbo].Clients (NoLock) Cl
			  on a.PatientPk = Cl.PatientPk and a.SiteCode = Cl.SiteCode
			  where a.TracingType is not null and a.TracingOutcome is not null
			  ) AS b 
			ON(
				a.PatientPK  = b.PatientPK 
			and a.SiteCode = b.SiteCode	
			and a.[TracingDate] = b.[TracingDate]
			and a.HtsNumber  = b.HtsNumber
			and a.TracingType  = b.TracingType
			and a.TracingOutcome = b.TracingOutcome
			and a.FacilityName  = b.FacilityName
			)
	WHEN NOT MATCHED THEN 
		INSERT(FacilityName,SiteCode,PatientPk,HtsNumber,Emr,Project,TracingType,TracingDate,TracingOutcome,LoadDate)  
		VALUES(FacilityName,SiteCode,PatientPk,HtsNumber,Emr,Project,TracingType,TracingDate,TracingOutcome,Getdate())

	WHEN MATCHED THEN
		UPDATE SET 
				a.[FacilityName]	=b.[FacilityName],
				
				a.[TracingType]		=b.[TracingType],
				a.[TracingDate]		=b.[TracingDate],
				a.[TracingOutcome]	=b.[TracingOutcome];
END

