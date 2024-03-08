IF OBJECT_ID(N'[NDWH].[dbo].[DimHTSTraceType]', N'U') IS NOT NULL 
	DROP TABLE [NDWH].[dbo].[DimHTSTraceType];

BEGIN
    with source_data as (
        select distinct TracingType as TraceType from ODS.dbo.HTS_ClientTracing
        union
        select distinct TraceType from ODS.dbo.HTS_PartnerTracings
    )
    select
       distinct 
       TraceTypeKey = IDENTITY(INT, 1, 1),
       source_data.*,
       cast(getdate() as date) as LoadDate
    into [NDWH].[dbo].[DimHTSTraceType]
	from source_data;

	alter table NDWH.dbo.DimHTSTraceType add primary key(TraceTypeKey);

END