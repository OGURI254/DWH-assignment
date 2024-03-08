IF OBJECT_ID(N'[NDWH].[dbo].[DimTestKitName]', N'U') IS NOT NULL 
	DROP TABLE [NDWH].[dbo].[DimTestKitName];
BEGIN

with source_data as (
    select distinct TestKitName1 as TestKitName from ODS.dbo.HTS_TestKits
    where TestKitName1 is not null and TestKitName1 <> '' and TestKitName1 <> 'null'
        union
    select distinct TestKitName2 as TestKitName from ODS.dbo.HTS_TestKits
    where TestKitName2 is not null and TestKitName2 <> '' and TestKitName2 <> 'null'
)
select 
    TestKitNameKey = IDENTITY(INT, 1, 1),
    source_data.*,
    cast(getdate() as date) as LoadDate
into NDWH.dbo.DimTestKitName
from source_data

alter table NDWH.dbo.DimTestKitName add primary key(TestKitNameKey); 

END