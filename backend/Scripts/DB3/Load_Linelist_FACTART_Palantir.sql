BEGIN

	
		CREATE TABLE [REPORTING].[dbo].[Linelist_FACTART_Palantir](
																	[PatientIDHash] [nvarchar](100) NULL,
																	[PatientPKHash] [nvarchar](100) NULL,	
																	[SiteCode] [int] NULL,
																	Gender  nvarchar(20),
																	UniquePatientIDGuidHash  nvarchar(150),

																   ) 

		INSERT INTO [REPORTING].[dbo].[Linelist_FACTART_Palantir]([PatientIDHash],[PatientPKHash],[SiteCode],Gender,UniquePatientIDGuidHash)
		SELECT
			a.PatientIDHash,
			a.PatientPKHash,
			a.SiteCode,
			a.Gender,
			--convert(nvarchar(64), hashbytes('SHA2_256', cast(b.Id  as nvarchar(36))), 2) UniquePatientIDGuid
			CONVERT(NVARCHAR(64), HASHBYTES('SHA2_256', UPPER(CAST(b.id as NVARCHAR(36)))), 2) as PatientID
		FROM REPORTING.DBO.Linelist_FACTART a
		LEFT JOIN [ODS].[DBO].CT_Patient b
		ON a.SiteCode = b.SiteCode and a.PatientPKHash = b.PatientPKHash
		WHERE county in ('Bomet','Uasin Gishu', 'Siaya') and ARTOutcomeDescription='Active'

END


