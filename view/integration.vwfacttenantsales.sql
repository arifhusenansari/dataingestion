CREATE VIEW integration.[vwfacttenantsales]
AS

SELECT
	dTenant.tenant_uuid as tenant_uuid
	,fact.year					AS year
	,fact.month					AS month
	,fact.source_main			AS source_main
	,fact.source_sub			AS source_sub
	,COALESCE(NULLIF(fact.source_main,0),fact.source_sub,0 ) AS final_sales 
	,fact.source_system			AS source_system
	,fact.etl_lastmodified_date AS etl_lastmodified_date
FROM [correction].[correction_lease_sales] fact
LEFT JOIN [enrichment].[dimtenant] dTenant
ON CONCAT(fact.l_id,'-',fact.source_system) = dTenant.tenant_uuid
