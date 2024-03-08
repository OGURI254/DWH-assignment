IF OBJECT_ID(N'REPORTING.dbo.all_EMRSites', N'U') IS NOT NULL 
	DROP TABLE REPORTING.dbo.all_EMRSites;
With EMRSites as (
  SELECT 
    fac.MFLCode, 
    fac.FacilityName, 
    fac.County, 
    fac.SubCounty, 
    a.SDP_Agency as AgencyName, 
    a.SDP as PartnerName, 
    fac.Latitude, 
    fac.Longitude, 
    fac.EMR, 
    fac.isCT, 
    fac.isPkv, 
    fac.isHts, 
	a.EMR_Status,
    CAST(
      GETDATE() AS DATE
    ) AS LoadDate 
  FROM 
    NDWH.dbo.DimFacility fac 
    JOIN ODS.dbo.All_EMRSites a on a.MFL_Code = fac.MFLCode
), 
ML AS (
  select 
    distinct SiteCode, 
    COALESCE(
      CASE WHEN SiteCode IS NOT NULL THEN 1 ELSE NULL END, 
      0
    ) AS isHTS_ML 
  from 
    ODS.dbo.HTS_EligibilityExtract 
  where 
    HIVRiskCategory is not null
) 
Select 
  MFLCode, 
  FacilityName, 
  County, 
  SubCounty, 
  AgencyName, 
  PartnerName, 
  Latitude, 
  Longitude, 
  EMR, 
  isCT, 
  isPkv, 
  isHts, 
  COALESCE(
    CASE WHEN SiteCode IS NOT NULL THEN 1 ELSE NULL END, 
    0
  ) AS isHTS_ML ,
  EMR_Status

  INTO REPORTING.dbo.all_EMRSites 
from 
  EMRSites 
  left join ML on ML.SiteCode = EMRSites.MFLCode