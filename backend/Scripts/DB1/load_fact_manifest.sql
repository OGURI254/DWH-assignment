IF OBJECT_ID(N'NDWH.DBO.Fact_manifest ', N'U') IS NOT NULL 
	DROP TABLE NDWH.DBO.Fact_manifest ;
BEGIN
	with Fact_manifest  as(
		Select
			max(m.id) manifestId,
			Cast(max(m.DateRecieved) as date) timeId,
			max(m.SiteCode) facilityId,
			max(coalesce(h.emr,'Unkown')) emrId,
			'CT' docketId,
			1 upload,
			cast(getdate() as date) as LoadDate
		from dwapicentral.dbo.facilitymanifest m 
			inner join his_implementation.dbo.all_emrsites h
		on m.SiteCode=h.MFL_Code
						GROUP BY	YEAR(m.DateRecieved), 
					MONTH(m.DateRecieved), SiteCode 

		UNION ALL 

		SELECT	MAX(m.id) AS manifestId, 
				CAST(MAX(m.DateArrived) AS DATE) AS timeId,
				MAX(m.SiteCode) AS facilityId, 
				MAX(COALESCE(h.emr, 'Unknown')) AS emrId, 
				'HTS' AS docketId, 
				1 AS upload,
				cast(getdate() as date) as LoadDate
		FROM htscentral.DBO.manifests m 
			INNER JOIN his_implementation.DBO.all_emrsites h ON m.SiteCode = h.MFL_Code 
		GROUP BY	YEAR(DateArrived), 
					MONTH(DateArrived), SiteCode 

			UNION ALL 

		SELECT MAX(m.id) AS manifestId, 
				CAST(MAX(m.DateArrived) AS DATE) AS timeId, 
				MAX(m.SiteCode) AS facilityId,
				MAX(COALESCE(h.emr, 'Unknown')) AS emrId,
				'PKV' AS docketId, 
				1 AS upload ,
				 cast(getdate() as date) as LoadDate
		FROM cbscentral.DBO.manifests m INNER JOIN his_implementation.DBO.all_emrsites h ON m.SiteCode = h.MFL_Code 
		GROUP BY YEAR(DateArrived), 
					MONTH(DateArrived), SiteCode
	)
	SELECT * INTO NDWH.DBO.Fact_manifest 
	FROM Fact_manifest
END
