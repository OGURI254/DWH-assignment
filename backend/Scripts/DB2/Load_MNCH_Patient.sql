
BEGIN
    --truncate table [ODS].[dbo].[MNCH_Patient]
	MERGE [ODS].[dbo].[MNCH_Patient] AS a
			USING(
					SELECT distinct P.[PatientPk],P.[SiteCode],P.[Emr],[Project],[Processed],[QueueId],[Status],[StatusDate],[DateExtracted]
						  ,[FacilityId],[FacilityName],[Pkv],[PatientMnchID],[PatientHeiID],[Gender],[DOB],[FirstEnrollmentAtMnch],[Occupation]
						  ,[MaritalStatus],[EducationLevel],[PatientResidentCounty],[PatientResidentSubCounty],[PatientResidentWard],[InSchool]
						  ,[Date_Created],[Date_Last_Modified],[NUPI]
					  FROM [MNCHCentral].[dbo].[MnchPatients] P(nolock)
					   inner join (select tn.PatientPK,tn.SiteCode,max(tn.DateExtracted)MaxDateExtracted FROM [MNCHCentral].[dbo].[MnchPatients] (NoLock)tn
					group by tn.PatientPK,tn.SiteCode)tm
					on P.PatientPk = tm.PatientPk and p.SiteCode = tm.SiteCode and p.DateExtracted = tm.MaxDateExtracted
					  INNER JOIN [MNCHCentral].[dbo].[Facilities] F ON P.[FacilityId] = F.Id) AS b 
						ON(
						 a.PatientPK  = b.PatientPK 
						and a.SiteCode = b.SiteCode
						
							)
					WHEN NOT MATCHED THEN 
						INSERT(PatientPk,SiteCode,Emr,Project,DateExtracted,FacilityName,Pkv,PatientMnchID,PatientHeiID,Gender,DOB,FirstEnrollmentAtMnch,Occupation,MaritalStatus,EducationLevel,PatientResidentCounty,PatientResidentSubCounty,PatientResidentWard,InSchool,Date_Created,Date_Last_Modified,NUPI,LoadDate)  
						VALUES(PatientPk,SiteCode,Emr,Project,DateExtracted,FacilityName,Pkv,PatientMnchID,PatientHeiID,Gender,DOB,FirstEnrollmentAtMnch,Occupation,MaritalStatus,EducationLevel,PatientResidentCounty,PatientResidentSubCounty,PatientResidentWard,InSchool,Date_Created,Date_Last_Modified,NUPI,Getdate())
				
					WHEN MATCHED THEN
						UPDATE SET 
							a.Gender = b.Gender,
							a.DOB = b.DOB,
							a.FirstEnrollmentAtMnch = b.FirstEnrollmentAtMnch,
							a.Occupation = b.Occupation,
							a.MaritalStatus = b.MaritalStatus,
							a.EducationLevel = b.EducationLevel,
							a.PatientResidentCounty = b.PatientResidentCounty,
							a.PatientResidentSubCounty = b.PatientResidentSubCounty,
							a.PatientResidentWard = b.PatientResidentWard,
							a.InSchool = b.InSchool;

				with cte AS (
						Select
						Sitecode,
						PatientPK,

						 ROW_NUMBER() OVER (PARTITION BY PatientPK,Sitecode ORDER BY
						PatientPK,Sitecode) Row_Num
						FROM  [ODS].[dbo].[MNCH_Patient](NoLock)
						)
						delete from cte 
						Where Row_Num >1 ;
END


