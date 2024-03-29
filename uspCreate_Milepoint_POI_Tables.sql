USE [KYTCVectorLoader_HIS]
GO


-- Remove old records
TRUNCATE TABLE dbo.MILEPOINT_POI_ALL
TRUNCATE TABLE dbo.MILEPOINT_POI_MILE
TRUNCATE TABLE dbo.MILEPOINT_POI_HALFMILE

-- Populate table fields from base
INSERT INTO dbo.MILEPOINT_POI_ALL (RT_UNIQUE, MILEPOINT, LATITUDE, LONGITUDE, COUNTY)
SELECT t1.[RT_UNIQUE],
	t1.MILEPOINT,
	[KYTCVector_HIS].[dbo].[udf_get_latitude](t1.[GEOLOC].STX,t1.[GEOLOC].STY),
	[KYTCVector_HIS].[dbo].[udf_get_longitude](t1.[GEOLOC].STX,t1.[GEOLOC].STY),
	t3.[Cnty_Name_UC]
FROM [dbo].[TENTH_MILE_ALL] t1
JOIN [KYTCVector_Reference].[dbo].LOOKUP_COUNTY t3
	ON t1.[CO_NUMBER] = t3.[Cnty_Number]

UPDATE t1
SET t1.[ROUTE] = t2.[ROUTE_LBL],
	t1.[RD_NAME] = t2.RD_NAME
FROM dbo.MILEPOINT_POI_ALL t1
JOIN [dbo].[KYALLROADS] t2
	on t1.[RT_UNIQUE] = t2.[RT_UNIQUE]

UPDATE t1
SET t1.CITY = t4.[NAME]
FROM dbo.MILEPOINT_POI_ALL t1
JOIN dbo.TENTH_MILE_ALL t2
	ON t1.RT_UNIQUE = t2.RT_UNIQUE
JOIN [KYTCVector_Reference].[dbo].[CORPORATE_BOUNDARIES_POLYGON] t4
	ON (t2.[GEOLOC]).STIntersects(t4.[SHAPE]) = 1

INSERT INTO dbo.MILEPOINT_POI_MILE
SELECT * FROM dbo.MILEPOINT_POI_ALL
WHERE CONVERT(nvarchar,MILEPOINT) LIKE ('%.0')

INSERT INTO dbo.MILEPOINT_POI_HALFMILE
SELECT * FROM dbo.MILEPOINT_POI_ALL
WHERE CONVERT(nvarchar,MILEPOINT) LIKE ('%.5')

INSERT INTO dbo.MILEPOINT_POI_CROSSOVERS (RT_UNIQUE, MILEPOINT, LATITUDE, LONGITUDE, COUNTY)
SELECT t1.[RT_UNIQUE], 
	t1.MILEPOINT,
	[KYTCVector_HIS].[dbo].[udf_get_latitude](t2.[GEOLOC].STX,t2.[GEOLOC].STY),
	[KYTCVector_HIS].[dbo].[udf_get_longitude](t2.[GEOLOC].STX,t2.[GEOLOC].STY),
	t3.[Cnty_Name_UC]
FROM [dbo].[TURNAROUND_POI] t1
JOIN [dbo].[NODES] t2
	ON t1.POINT_ID = t2.POINT_ID
JOIN [KYTCVector_Reference].[dbo].LOOKUP_COUNTY t3
	ON CAST(SUBSTRING(t1.[RT_UNIQUE], 1,3) AS int) = t3.[Cnty_Number]

UPDATE t1
SET t1.[ROUTE] = t2.[ROUTE_LBL],
	t1.[RD_NAME] = t2.RD_NAME
FROM dbo.MILEPOINT_POI_CROSSOVERS t1
JOIN [dbo].[KYALLROADS] t2
	on t1.[RT_UNIQUE] = t2.[RT_UNIQUE]

UPDATE t1
SET t1.CITY = t4.[NAME]
FROM dbo.MILEPOINT_POI_CROSSOVERS t1
JOIN dbo.TENTH_MILE_ALL t2
	ON t1.RT_UNIQUE = t2.RT_UNIQUE
JOIN [KYTCVector_Reference].[dbo].[CORPORATE_BOUNDARIES_POLYGON] t4
	ON (t2.[GEOLOC]).STIntersects(t4.[SHAPE]) = 1



GO

