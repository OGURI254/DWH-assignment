BEGIN
	with uploads as (
		SELECT 
			'CT' AS docket,
			f.County AS county,
			f.SubCounty AS subcounty,
			SDP_Agency AS agency,
			SDP AS partner,
			COUNT(DISTINCT MFLCode) AS expected
		FROM
			NDWH.DBO.DimFacility f
			INNER JOIN ODS.dbo.All_EMRSites a on a.MFL_Code = MFLCode
		WHERE
			(isCT = 1)
		GROUP BY f.county, f.subCounty, SDP_Agency, SDP
		UNION 
		SELECT 
			'HTS' AS docket,
			h.County AS county,
			h.subCounty AS subcounty,
			SDP_Agency AS agency,
			SDP AS partner,
			COUNT(DISTINCT MFLCode) AS expected
		FROM
			NDWH.DBO.DimFacility h
			INNER JOIN ODS.dbo.All_EMRSites a on a.MFL_Code = MFLCode
		WHERE
			(isHts = 1)
		GROUP BY h.county, h.subCounty, SDP_Agency, SDP
		UNION 
		SELECT 
			'PKV' AS docket,
			p.county AS county,
			p.subCounty AS subcounty,
			SDP_Agency AS agency,
			SDP AS partner,
			COUNT(DISTINCT MFLCode) AS expected
		FROM
			NDWH.DBO.DimFacility p
			INNER JOIN ODS.dbo.All_EMRSites a on a.MFL_Code = MFLCode
		WHERE
			(isPkv = 1)
		GROUP BY p.county, p.subCounty, SDP_Agency, SDP
	) 
	select * , cast(getdate() as date) as LoadDate into REPORTING.DBO.AggregateExpectedUploads  from uploads
END