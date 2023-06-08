CREATE VIEW integration.[vwfacttenantcontract]
AS

SELECT
	dTenant.tenant_uuid as tenant_uuid
	,fact.area_persqft			AS area_persqft
	,fact.monthly_rent			 AS monthly_rent
	,fact.monthly_rent *12			 AS annual_rent
	,fact.lease_from			 AS lease_from
	,fact.lease_to				 AS lease_to
	,fact.source_system			 AS source_system
	,fact.etl_lastmodified_date	 AS etl_lastmodified_date
FROM [correction].[correction_lease_trans] fact
LEFT JOIN [enrichment].[dimtenant] dTenant
ON CONCAT(fact.l_id,'-',fact.source_system) = dTenant.tenant_uuid