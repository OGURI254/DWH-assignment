GO

CREATE PROCEDURE [dbo].[generate_consistency_uploads_nw]
@PERIOD VARCHAR(55),
@docketName VARCHAR(55)
AS
BEGIN
SELECT 
	COUNT(DISTINCT facilityId) AS consistency, 
	docket, 
	county, 
	agency, 
	partner,
	DATEADD(MONTH, -1, DATEADD( DAY , 1, EOMONTH(DATEADD(MONTH, -2, @PERIOD)))) AS startPeriod,
	EOMONTH(@PERIOD) AS endPeriod
FROM (
	SELECT fm.facilityId,
		fm.docketid AS docket,
		f.county,
		f.AgencyName agency,
		f.PartnerName partner
	FROM NDWH.dbo.fact_manifest fm 
		JOIN REPORTING.dbo.all_EMRSites f ON fm.facilityId = f.MFLCode
	WHERE fm.docketid = @docketName AND fm.timeId BETWEEN DATEADD (MONTH, -1, DATEADD ( DAY, 1,EOMONTH ( DATEADD(MONTH, -2, @PERIOD))) ) AND EOMONTH( @PERIOD ) 
) X
GROUP BY facilityId, docket, county, agency, partner
HAVING COUNT(facilityId) >= 3;
END
GO

