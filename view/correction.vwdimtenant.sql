CREATE VIEW [correction].[vwdimtenant]
AS
SELECT
	 CONCAT(ld.l_id,'-',ld.source_system) as  tenant_uuid
	,ld.l_id as tenant_id 
	,ld.lease_type as lease_type
	,ld.lease_status as  lease_status
	,ld.category as  category
	,ld.source_system as source_system
	,ld.etl_lastmodified_date etl_lastmodified_date

FROM [correction].[correction_lease_details] ld