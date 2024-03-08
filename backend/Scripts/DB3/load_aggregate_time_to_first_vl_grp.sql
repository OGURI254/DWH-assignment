GO

select 
    MFLCode,
    f.FacilityName,
    County,
    Subcounty,
    p.PartnerName,
    a.AgencyName,
    Year(StartARTDateKey) StartART_Year,
    DateName(Month,StartARTDateKey) StartART_Month,
    EOMONTH(date.[Date]) as AsOfDate,
    TimeToFirstVLGrp,
    Count(*) as NumPatients,
    SUM(Count(*)) OVER (PARTITION BY MFLCode,Year(StartARTDateKey),DateName(Month,StartARTDateKey)) AS TotalBySite,
    cast((cast(Count(*) as decimal (9,2))/
        SUM(Count(*)) OVER (PARTITION BY MFLCode,Year(StartARTDateKey),DateName(Month,StartARTDateKey))*100) 
    as decimal(8,2))  AS proportions,
    CAST(GETDATE() AS DATE) AS LoadDate 
 INTO REPORTING.[dbo].[AggregateTimeToFirstVLGrp]
FROM NDWH.dbo.FactViralLoads it
INNER join NDWH.dbo.DimAgeGroup g on g.AgeGroupKey=it.AgeGroupKey
INNER join NDWH.dbo.DimFacility f on f.FacilityKey = it.FacilityKey
INNER JOIN NDWH.dbo.DimAgency a on a.AgencyKey = it.AgencyKey
INNER JOIN NDWH.dbo.DimPatient pat on pat.PatientKey = it.PatientKey
INNER JOIN NDWH.dbo.DimPartner p on p.PartnerKey = it.PartnerKey
INNER JOIN NDWH.dbo.FactART art on art.PatientKey = it.PatientKey
INNER JOIN NDWH.dbo.DimDate as date on date.DateKey = art.StartARTDateKey
Group BY 
    MFLCode,
    f.FacilityName,
    County,
    Subcounty,
    p.PartnerName,
    a.AgencyName,
    Year(StartARTDateKey),
    TimeToFirstVLGrp,
    DateName(Month,StartARTDateKey),
    EOMONTH(date.Date)
order by 
    MFLCode,
    Year(StartARTDateKey),
    TimeToFirstVLGrp