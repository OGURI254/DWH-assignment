IF OBJECT_ID(N'[NDWH].[dbo].[DimHTSTraceOutcome]', N'U') IS NOT NULL 
	DROP TABLE [NDWH].[dbo].[DimHTSTraceOutcome];

BEGIN

with source_data as (
    select distinct TracingOutcome as TraceOutcome from ODS.dbo.HTS_ClientTracing 
    where  TracingOutcome <> 'null' and TracingOutcome <> ''
    union
    select distinct TraceOutcome from ODS.dbo.HTS_PartnerTracings
    where  TraceOutcome <> 'null' and TraceOutcome <> ''
)
select 
    distinct
    TraceOutcomeKey = IDENTITY(INT, 1, 1), 
    case
        when source_data.TraceOutcome in ('Contact Not Reached', 'Contacted and not Reached') then 'Contact Not Reached'
        else source_data.TraceOutcome
    end as TraceOutcome,
    cast(getdate() as date) as LoadDate
into [NDWH].[dbo].[DimHTSTraceOutcome]
from source_data;

alter table NDWH.dbo.DimHTSTraceOutcome add primary key(TraceOutcomeKey);

END