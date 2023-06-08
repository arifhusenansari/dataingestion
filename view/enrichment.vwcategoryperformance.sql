CREATE VIEW enrichment.vwcategoryperformance
AS

WITH Current_Tenant 
as
(
	SELECT 
		tenant_uuid as [Tenant UUID]
		,[tenant_id] as [Tenant ID]
		,lease_type as [Lease Type]
		,category as [Catagory]
	FROM [enrichment].[dimtenant]
	

),AnnualRent
AS
(
	SELECT
		tenant_uuid as [Tenant UUID]
		,annual_rent as [Annual Rent] 
		,area_persqft as [Area Per Sqft]
	FROM [enrichment].[facttenantcontract] t1
	INNER JOIN Current_Tenant  ct
	on t1.tenant_uuid = ct.[Tenant UUID]
)
,Sales
AS
(
	SELECT 
		tenant_uuid [Tenant UUID] ,sum(final_sales) [Sales] 
	FROM [enrichment].[facttenantsales] 
	GROUP BY tenant_uuid
)

SELECT
	ct.Catagory
	,sum(ar.[Annual Rent]) [Rent_per_Category]	,sum(ar.[Area Per Sqft]) [Area_SqFt_per_Category]	,SUM(s.Sales) as  [Sales_Per_Category]
	,null as etl_lastmodified_date
FROM Current_Tenant ct
INNER JOIN AnnualRent ar
ON ct.[Tenant UUID] = ar.[Tenant UUID]
LEFT JOIN Sales s
ON ct.[Tenant UUID]=s.[Tenant UUID]
GROUP BY ct.Catagory