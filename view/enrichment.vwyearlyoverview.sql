CREATE VIEW enrichment.vwyearlyoverview
AS
WITH 

Current_Tenant 
as
(
	SELECT 
		tenant_uuid as [Tenant UUID]
		,[tenant_id] as [Tenant ID]
		,lease_type as [Lease Type]
		,category as [Catagory]
	FROM [enrichment].[dimtenant]
	WHERE [lease_status]='current'

)

,AnnualRent
AS
(
	SELECT
		tenant_uuid as [Tenant UUID]
		,annual_rent as [Annual Rent] 
		,lease_from as [Lease From]
		,lease_to as [Lease To]
		,area_persqft as [Area Per Sqft]
	FROM [enrichment].[facttenantcontract] t1
	INNER JOIN Current_Tenant  ct
	on t1.tenant_uuid = ct.[Tenant UUID]
)
,Latest12MonthSales AS
(
	SELECT 
		tenant_uuid,Year,MOnth,final_sales
		,sum(final_sales) OVER (PARTITION BY tenant_uuid ORDER BY year DESC,month DESC ROWS BETWEEN current row AND 11 following ) Last12MonthSales
		,COUNT(NULLIF(final_sales,0)) OVER (PARTITION BY tenant_uuid ORDER BY year DESC,month DESC ROWS BETWEEN current row AND 11 following ) as NumberOfMonthWithSalesInLast12Month
		,ROW_NUMBER() OVER (PARTITION BY tenant_uuid ORDER BY year DESC,month DESC )  LatestCalculationSeq
	FROM [enrichment].[facttenantsales] 
	--WHERE tenant_uuid='ABC0005533-systmem_1'
)
, AnnualizeSales
as
(
	SELECT 
		tenant_uuid as [Tenant UUID],CASE WHEN NumberOfMonthWithSalesInLast12Month <>0 THEN CAST((Last12MonthSales/NumberOfMonthWithSalesInLast12Month)*12 as NUMERIC(18,3)) 
		ELSE 0 END As [Annual Sales]
	FROM Latest12MonthSales
	WHERE LatestCalculationSeq=1
)


SELECT 
	 ct.[Tenant ID]	 AS		[Tenant_ID]
	,ct.[Lease Type] AS		[Lease_Type]
	,ct.[Catagory]	 AS		[Catagory]
	,r.[Area Per Sqft] AS	[Area_Per_Sqft]
	,r.[Annual Rent]   AS	[Annual_Rent]
	,r.[Lease From]	   AS	[Lease_From]
	,r.[Lease To]	   AS	[Lease_To]
	,s.[Annual Sales]  AS	[Annual_Sales]
	,null as etl_lastmodified_date
FROM Current_Tenant ct
INNER JOIN AnnualRent r
ON r.[Tenant UUID]= ct.[Tenant UUID]
INNER JOIN AnnualizeSales s
ON s.[Tenant UUID] = ct.[Tenant UUID]