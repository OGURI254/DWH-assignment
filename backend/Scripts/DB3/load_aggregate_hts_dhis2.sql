BEGIN
	SELECT 
		Y.* 
	INTO REPORTING.dbo.AggregateFACT_HTS_DHIS2
	FROM ( 
		SELECT
			[id]
			,[DHISOrgId]
			,[SiteCode]
			,[FacilityName]
			,CT.County
			,CT.SubCounty
			,[Ward]
			,[ReportMonth_Year]
			,[Tested_Total]
			,[Positive_Total]
			,[createdAt]
			,[updatedAt]
			,[Tested_1_9]
			,[Tested_10_14_M]
			,[Tested_10_14_F]
			,[Tested_15_19_M]
			,[Tested_15_19_F]
			,[Tested_20_24_M]
			,[Tested_20_24_F]
			,[Tested_25_Plus_M]
			,[Tested_25_Plus_F]
			,[Positive_1_9]
			,[Positive_10_14_M]
			,[Positive_10_14_F]
			,[Positive_15_19_M]
			,[Positive_15_19_F]
			,[Positive_20_24_M]
			,[Positive_20_24_F]
			,[Positive_25_Plus_M]
			,[Positive_25_Plus_F]
			,Sites.SDP PartnerName
			,Sites.SDP_Agency AgencyName
			,CAST(GETDATE() AS DATE) AS LoadDate
		FROM NDWH.dbo.FACT_HTS_DHIS2 CT
		LEFT JOIN ODS.dbo.ALL_EMRSites Sites on CT.SiteCode=Sites.MFL_Code
	)Y 
	WHERE PartnerName IS NOT NULL

END
