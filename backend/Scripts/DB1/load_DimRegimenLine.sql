IF OBJECT_ID(N'[NDWH].[dbo].[DimRegimenLine]', N'U') IS NOT NULL 
	DROP TABLE [NDWH].[dbo].[DimRegimenLine];
BEGIN
	with source_regimen_line as (
		select 
			distinct StartRegimenLine as RegimenLine 
		from ODS.dbo.CT_ARTPatients
		union all
		select 
			distinct ltrim(rtrim(LastRegimenLine)) as RegimenLine 
		from ODS.dbo.CT_ARTPatients
	),
	enrcihed_source_regimen_line as (
		select
			distinct RegimenLine,
			case 
				when RegimenLine in ('1st line','Adult ART FirstLine','Adult first line','Adult FirstLine','Child first line','First line','First line substitute','Paeds ART FirstLine','1st Alternative', 'Child FirstLine') then 'First Line'
				when RegimenLine in ('Adult ART SecondLine','Adult second line','Adult SecondLine','Child SecondLine','Paeds ART Secondline','Second line','Second line substitute', '2nd Line') then 'Second Line'
				when RegimenLine in ('Adult ART ThirdLine ','Adult ThirdLine','Child ThirdLine','Third line','Adult ART ThirdLine') then 'Third Line'
				when RegimenLine in ('unknown') then 'Unknown'
				when RegimenLine in ('PMTCT Maternal Regimens', 'PMTCT Regimens') then 'PMTCT'
				when RegimenLine in ('Other') then 'Other'
				else 'Unknown'
			end as RegimenLineCategory
		from source_regimen_line
		where RegimenLine is not null and ltrim(rtrim(RegimenLine)) != '' and RegimenLine != 'null' and len(RegimenLine) > 1
	)
	select 
		RegimenLineKey = IDENTITY(INT, 1, 1),
		enrcihed_source_regimen_line.*,
		cast(getdate() as date) as LoadDate
	into [NDWH].[dbo].[DimRegimenLine]
	from enrcihed_source_regimen_line;

	alter table NDWH.dbo.DimRegimenLine add primary key(RegimenLineKey);
END