SELECT 
	Y.* 
INTO REPORTING.dbo.AggregateFACT_CT_DHIS2
FROM ( 
	SELECT
		CT.id
		,CT.DHISOrgId
		,CT.SiteCode
		,CT.FacilityName
		,CT.County
		,CT.SubCounty
		,CT.Ward
		,CT.ReportMonth_Year
		,CT.Enrolled_Total
		,CT.StartedART_Total
		,CT.CurrentOnART_Total
		,CT.CTX_Total
		,CT.OnART_12Months
		,CT.NetCohort_12Months
		,CT.VLSuppression_12Months
		,CT.VLResultAvail_12Months
		,CT.createdAt
		,CT.updatedAt
		,CT.Start_ART_Under_1
		,CT.Start_ART_1_9
		,CT.Start_ART_10_14_M
		,CT.Start_ART_10_14_F
		,CT.Start_ART_15_19_M
		,CT.Start_ART_15_19_F
		,CT.Start_ART_20_24_M
		,CT.Start_ART_20_24_F
		,CT.Start_ART_25_Plus_M
		,CT.Start_ART_25_Plus_F
		,CT.On_ART_Under_1
		,CT.On_ART_1_9
		,CT.On_ART_10_14_M
		,CT.On_ART_10_14_F
		,CT.On_ART_15_19_M
		,CT.On_ART_15_19_F
		,CT.On_ART_20_24_M
		,CT.On_ART_20_24_F
		,CT.On_ART_25_Plus_M
		,CT.On_ART_25_Plus_F
		,Sites.SDP PartnerName
		,Sites.[SDP_Agency] AgencyName
		,cast(getdate() as date) as LoadDate
	FROM NDWH.dbo.FACT_CT_DHIS2 CT
	LEFT JOIN HIS_Implementation.dbo.ALL_EMRSites Sites on CT.SiteCode COLLATE Latin1_General_CI_AS=Sites.MFL_Code
)Y 
WHERE PartnerName IS NOT NULL