GO

with AdverseEvents as (
    SELECT
            MFLCode,
            pat.PatientKey,
            g.DATIMAgeGroup,
            pat.Gender,
            f.FacilityName,
            County,
            SubCounty,
            p.PartnerName,
            a.AgencyName,
            AdverseEvent,
            AdverseEventCause,
            AdverseEventRegimen,
            AdverseEventActionTaken,
            Severity
        FROM
            [NDWH].dbo.FactAdverseEvents it
            INNER join NDWH.dbo.DimFacility f on f.FacilityKey = it.FacilityKey
            INNER JOIN NDWH.dbo.DimAgency a on a.AgencyKey = it.AgencyKey
            INNER JOIN NDWH.dbo.DimPatient pat on pat.PatientKey = it.PatientKey
            INNER JOIN NDWH.dbo.DimPartner p on p.PartnerKey = it.PartnerKey
            INNER JOIN NDWH.dbo.FactART art on art.PatientKey = it.PatientKey
            LEFT join NDWH.dbo.DimAgeGroup g on g.Age = art.AgeLastVisit
        WHERE
            pat.IsTXCurr = 1
)

SELECT
    MFLCode,
    DATIMAgeGroup,
    Gender,
    FacilityName,
    County,
    Subcounty,
    PartnerName,
    AgencyName,
    AdverseEvent,
    AdverseEventCause,
    AdverseEventActionTaken,
    AdverseEventRegimen,
    Severity,
	count(*) as AdverseEventsCount,
	count(DISTINCT PatientKey) as AdverseClientsCount,
    cast(getdate() as date) as LoadDate
INTO [REPORTING].[dbo].AggregateAdverseEvents 
FROM AdverseEvents
GROUP BY
    MFLCode,
    DATIMAgeGroup,
    Gender,
    FacilityName,
    County,
    Subcounty,
    PartnerName,
    AgencyName,
    AdverseEvent,
    AdverseEventCause,
    AdverseEventActionTaken,
    AdverseEventRegimen,
    Severity
GO
