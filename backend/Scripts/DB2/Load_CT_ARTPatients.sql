BEGIN
	       ---- Refresh [ODS].[dbo].[CT_ARTPatients]
			MERGE [ODS].[dbo].[CT_ARTPatients] AS a
				USING(SELECT  P.[PatientCccNumber] AS PatientID, P.[PatientPID] AS PatientPK,F.Name AS FacilityName, F.Code AS SiteCode,PA.[AgeEnrollment] AgeEnrollment
					  ,PA.[AgeARTStart] AgeARTStart,PA.[AgeLastVisit] AgeLastVisit,PA.[RegistrationDate] RegistrationDate,PA.[PatientSource] PatientSource
					  ,PA.[StartARTDate] StartARTDate,PA.[PreviousARTStartDate] PreviousARTStartDate,PA.[PreviousARTRegimen] PreviousARTRegimen,PA.[StartARTAtThisFacility] StartARTAtThisFacility
					  ,PA.[StartRegimen] StartRegimen,PA.[StartRegimenLine] StartRegimenLine,PA.[LastARTDate] LastARTDate,PA.[LastRegimen] LastRegimen
					  ,PA.[LastRegimenLine] LastRegimenLine,PA.[Duration] Duration,PA.[ExpectedReturn] ExpectedReturn,PA.[LastVisit] LastVisit
					  ,PA.[ExitReason] ExitReason,PA.[ExitDate] ExitDate,P.[Emr] Emr
					  ,CASE P.[Project] 
								WHEN 'I-TECH' THEN 'Kenya HMIS II' 
								WHEN 'HMIS' THEN 'Kenya HMIS II'
							ELSE P.[Project] 
					   END AS [Project] 
					  --,PA.[PatientId]
					  ,PA.[Voided] Voided
					  ,PA.[Processed] Processed
					  ,PA.[DOB] DOB
					  ,PA.[Gender] Gender
					  ,PA.[Provider] [Provider]
					  ,PA.[Created] Created
					  ,LTRIM(RTRIM(STR(F.Code)))+'-'+LTRIM(RTRIM(P.[PatientCccNumber])) +'-'+LTRIM(RTRIM(STR(P.[PatientPID]))) AS PKV
					  , NULL AS [PatientUID]
					  ,PA.[PreviousARTUse] PreviousARTUse
					  ,PA.[PreviousARTPurpose] PreviousARTPurpose
					  ,PA.[DateLastUsed] DateLastUsed

					FROM [DWAPICentral].[dbo].[PatientExtract] P with(NoLock) 
					INNER JOIN [DWAPICentral].[dbo].[PatientArtExtract] PA with(NoLock) ON PA.[PatientId]= P.ID AND PA.Voided=0
					INNER JOIN [DWAPICentral].[dbo].[Facility] F with(NoLock)  ON P.[FacilityId] = F.Id AND F.Voided=0 
					--INNER JOIN FacilityManifest_MaxDateRecieved(NoLock) a ON F.Code = a.SiteCode
					---LEFT JOIN All_Staging_2016_2.dbo.stg_Patients TPat ON TPat.PKV=LTRIM(RTRIM(STR(F.Code)))+'-'+LTRIM(RTRIM(P.[PatientCccNumber]))+'-'+LTRIM(RTRIM(STR(P.[PatientPID])))
					--GROUP BY  F.Name , YEAR([StartARTDate])
					WHERE p.gender!='Unknown' ) AS b 
						ON(a.PatientID COLLATE SQL_Latin1_General_CP1_CI_AS = b.PatientID COLLATE SQL_Latin1_General_CP1_CI_AS
						and a.PatientPK  = b.PatientPK 
						and a.SiteCode = b.SiteCode)  ----add more checks to uniquely Identify a ARTPatient
						----lastArtDate,Duration,ExpectedReturn,LastRegimen,LastRegimenLine all the possible columns
			--WHEN MATCHED THEN
			--UPDATE SET 
			--a.FacilityName = B.FacilityName
			WHEN NOT MATCHED THEN 
			INSERT(PatientPK,PatientID,DOB,AgeEnrollment,AgeARTStart,AgeLastVisit,SiteCode,FacilityName,RegistrationDate,PatientSource,Gender,StartARTDate,PreviousARTStartDate,PreviousARTRegimen,StartARTAtThisFacility,StartRegimen,StartRegimenLine,LastARTDate,LastRegimen,LastRegimenLine,Duration,ExpectedReturn,Provider,LastVisit,ExitReason,ExitDate,Emr,Project,PKV,PatientUID,PreviousARTUse,PreviousARTPurpose,DateLastUsed) 
			VALUES(PatientPK,PatientID,DOB,AgeEnrollment,AgeARTStart,AgeLastVisit,SiteCode,FacilityName,RegistrationDate,PatientSource,Gender,StartARTDate,PreviousARTStartDate,PreviousARTRegimen,StartARTAtThisFacility,StartRegimen,StartRegimenLine,LastARTDate,LastRegimen,LastRegimenLine,Duration,ExpectedReturn,Provider,LastVisit,ExitReason,ExitDate,Emr,Project,PKV,PatientUID,PreviousARTUse,PreviousARTPurpose,DateLastUsed);
			
			---Remove any duplicate from [NDWH_DB].[dbo].[DimPatient]
			WITH CTE AS   
				(  
					SELECT [PatientPK],[SiteCode],ROW_NUMBER()  ----PARTITION by the columns
					OVER (PARTITION BY [PatientPK],[SiteCode] 
					ORDER BY [PatientPK],[SiteCode]) AS dump_ 
					FROM [ODS].[dbo].[CT_ARTPatients] 
					)  
			
			DELETE FROM CTE WHERE dump_ >1;

	END
