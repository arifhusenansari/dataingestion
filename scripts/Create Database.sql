USE master
-- Create Database 
IF DB_ID('raw_zone') IS NULL
BEGIN
	CREATE DATABASE raw_zone
END
IF DB_ID('quality_zone') IS NULL
BEGIN
	CREATE DATABASE quality_zone
END
IF DB_ID('curated_zone') IS NULL
BEGIN
	CREATE DATABASE curated_zone
END
IF DB_ID('metadata') IS NULL
BEGIN
	CREATE DATABASE metadata
END
GO
USE [quality_zone]

IF SCHEMA_ID('integration') IS NULL
BEGIN
	EXEC('CREATE SCHEMA integration')
END
GO


USE master

--- Create Meta data tables. 

IF OBJECT_ID('metadata.dbo.object') IS NOT NULL
BEGIN 
	DROP TABLE metadata.dbo.object
END 

CREATE TABLE metadata.dbo.Object(
	ObjectId smallint identity(1,1) NOT NULL,
	ObjectName varchar(200),
	ObjectType varchar(50),
	DatabaseName varchar(100),
	SchemaName varchar(100),
	SourcePath varchar(4000),
	ProcessedPath varchar(4000),
	ErrorPath varchar(4000),
	Extension varchar(5),
	RowDelim varchar(5),
	ColumnDelim varchar(5),
	TextQualifire varchar(3),
	IsActive bit,
	InsertedBy varchar(100),
	InsertedDate datetime2 DEFAULT( GETDATE()),
	ModifiedBy varchar(100),
	ModifiedDate datetime2 DEFAULT( GETDATE()),
		--PRIMARY KEY Pk_object_ObjectName_ObjectType_DatabaseName_SchemaName_SourcePath
)
INSERT INTO metadata.dbo.Object([ObjectName],[ObjectType],[DatabaseName],[SchemaName],[SourcePath],[ProcessedPath],[ErrorPath],[Extension],[ColumnDelim],[RowDelim],[TextQualifire],[IsActive],[InsertedBy],[ModifiedBy])

VALUES 
('Lease_Details','file',null,null,'e:\source','e:\source\processed','E:\source\error','csv',',','crlf','"','1','system','system')
,('Lease_Sales','file',null,null,'e:\source','e:\source\processed','E:\source\error','csv',',','crlf','"','1','system','system')
,('Lease_Trans','file',null,null,'e:\source','e:\source\processed','E:\source\error','csv',',','crlf','"','1','system','system')
,('landing_lease_details','table','raw_zone','landing',null,null,null,null,null,null,null,1,'system','system')
,('landing_lease_sales','table','raw_zone','landing',null,null,null,null,null,null,null,1,'system','system')
,('landing_lease_trans','table','raw_zone','landing',null,null,null,null,null,null,null,1,'system','system')

,('correction_lease_details','table','quality_zone','correction',null,null,null,null,null,null,null,1,'system','system')
,('correction_lease_sales','table','quality_zone','correction',null,null,null,null,null,null,null,1,'system','system')
,('correction_lease_trans','table','quality_zone','correction',null,null,null,null,null,null,null,1,'system','system')
,('vwdimtenant','view','quality_zone','correction',null,null,null,null,null,null,null,1,'system','system')
,('dimtenant','table','quality_zone','enrichment',null,null,null,null,null,null,null,1,'system','system')
,('vwfacttenantcontract','view','quality_zone','integration',null,null,null,null,null,null,null,1,'system','system')
,('facttenantcontract','table','quality_zone','enrichment',null,null,null,null,null,null,null,1,'system','system')
,('vwfacttenantsales','view','quality_zone','integration',null,null,null,null,null,null,null,1,'system','system')
,('facttenantsales','table','quality_zone','enrichment',null,null,null,null,null,null,null,1,'system','system')
,('facttenantsales','table','curated_zone','discovery',null,null,null,null,null,null,null,1,'system','system')
,('facttenantcontract','table','curated_zone','discovery',null,null,null,null,null,null,null,1,'system','system')
,('dimtenant','table','curated_zone','discovery',null,null,null,null,null,null,null,1,'system','system')
,('vwyearlyoverview','view','quality_zone','enrichment',null,null,null,null,null,null,null,1,'system','system')
,('yearlyoverview','table','curated_zone','discovery',null,null,null,null,null,null,null,1,'system','system')
,('vwcategoryperformance','view','quality_zone','enrichment',null,null,null,null,null,null,null,1,'system','system')
,('categoryperformance','table','curated_zone','discovery',null,null,null,null,null,null,null,1,'system','system')

IF OBJECT_ID('metadata.dbo.Task') IS NOT NULL
BEGIN 
	DROP TABLE metadata.dbo.Task
END 
CREATE TABLE metadata.dbo.Task (
	TaskId smallint identity(1,1) not null,
	SourceObjectId smallint not null,
	TargetObjectId smallint not null,
	JobName varchar(100) not null,
	IsActive bit,
	InsertedBy varchar(100),
	InsertedDate datetime2 DEFAULT( GETDATE()),
	ModifiedBy varchar(100),
	ModifiedDate datetime2 DEFAULT( GETDATE()),
)


INSERT INTO metadata.dbo.Task
           ([SourceObjectId]
           ,[TargetObjectId]
           ,[JobName]
           ,[IsActive]
           ,[InsertedBy]
           ,[ModifiedBy]
           )
VALUES 
(1,4,'sourcetolanding',1,'system','system')
,(2,5,'sourcetolanding',1,'system','system')
,(3,6,'sourcetolanding',1,'system','system')
,(4,7,'landingtocorrection',1,'system','system')
,(5,8,'landingtocorrection',1,'system','system')
,(6,9,'landingtocorrection',1,'system','system')
,(10,11,'correctiontoenrichment',1,'system','system')
,(12,13,'integrationtoenrichment',1,'system','system')
,(14,15,'integrationtoenrichment',1,'system','system')
,(13,17,'enrichmenttodiscovery',1,'system','system')
,(15,16,'enrichmenttodiscovery',1,'system','system')
,(11,18,'enrichmenttodiscovery',1,'system','system')
,(19,20,'enrichmenttodiscovery',1,'system','system')
,(21,22,'enrichmenttodiscovery',1,'system','system')




--- Create metadata.dbo.TaskColumnMappping table 

IF OBJECT_ID('metadata.dbo.TaskColumnMappping') IS NOT NULL
BEGIN 
	DROP TABLE metadata.dbo.TaskColumnMappping
END 
CREATE TABLE metadata.dbo.TaskColumnMappping (
	TaskColumnMappingID int identity (1,1) not null,
	TaskId smallint,
	SourceColumnName		varchar(100),
	SourceColumnType		varchar(100),
	SourceColumnLength		smallint,
	SourceColumnPrecision	smallint,
	SourceColumnScale smallint,
	TargetColumnName varchar(100),
	TargetColumnType varchar(100),
	TargetColumnLength smallint,
	TargetColumnPrecision smallint,
	TargetColumnScale smallint,
	IsPrimary bit,
	IsActive bit,
	InsertedBy varchar(100) DEFAULT('system'),
	InsertedDate datetime2 DEFAULT( GETDATE()),
	ModifiedBy varchar(100) DEFAULT('system'),
	ModifiedDate datetime2 DEFAULT( GETDATE()),
)

INSERT INTO metadata.dbo.TaskColumnMappping([TaskId],[SourceColumnName],[SourceColumnType],[SourceColumnLength],[SourceColumnPrecision],[SourceColumnScale],[TargetColumnName],[TargetColumnType],[TargetColumnLength],[TargetColumnPrecision],[TargetColumnScale],[IsPrimary],[IsActive])
VALUES 
(1,'L_ID','varchar',-1,null,null,'l_id','varchar',-1,null,null,0,1)
,(1,'lease_type','varchar',-1,null,null,'lease_type','varchar',-1,null,null,0,1)
,(1,'lease_status','varchar',-1,null,null,'lease_status','varchar',-1,null,null,0,1)
,(1,'Category','varchar',-1,null,null,'category','varchar',-1,null,null,0,1)

,(2,'L_ID','varchar',-1,null,null,'l_id','varchar',-1,null,null,0,1)
,(2,'year','varchar',-1,null,null,'year','varchar',-1,null,null,0,1)
,(2,'month','varchar',-1,null,null,'month','varchar',-1,null,null,0,1)
,(2,'Source_main','varchar',-1,null,null,'source_main','varchar',-1,null,null,0,1)
,(2,'Source_Sub','varchar',-1,null,null,'source_sub','varchar',-1,null,null,0,1)


,(3,'L_ID','varchar',-1,null,null,'l_id','varchar',-1,null,null,0,1)
,(3,'Area_PerSqFT','varchar',-1,null,null,'area_persqft','varchar',-1,null,null,0,1)
,(3,'Monthly_Rent','varchar',-1,null,null,'monthly_rent','varchar',-1,null,null,0,1)
,(3,'Lease_From','varchar',-1,null,null,'lease_from','varchar',-1,null,null,0,1)
,(3,'Lease_To','varchar',-1,null,null,'lease_to','varchar',-1,null,null,0,1)

,(4,'l_id','varchar',-1,null,null,'l_id','varchar',-1,null,null,1,1)
,(4,'lease_type','varchar',-1,null,null,'lease_type','varchar',-1,null,null,0,1)
,(4,'lease_status','varchar',-1,null,null,'lease_status','varchar',-1,null,null,0,1)
,(4,'Category','varchar',-1,null,null,'category','varchar',-1,null,null,0,1)
,(4,'''systmem_1''','varchar',-1,null,null,'source_system','varchar',-1,null,null,1,1)


,(5,'l_id','varchar',-1,null,null,'l_id','varchar',-1,null,null,1,1)
,(5,'year','varchar',-1,null,null,'year','smallint',-1,null,null,1,1)
,(5,'month','varchar',-1,null,null,'month','smallint',-1,null,null,1,1)
,(5,'source_main','varchar',-1,null,null,'source_main','numeric',null,18,3,0,1)
,(5,'source_sub','varchar',-1,null,null,'source_sub','numeric',null,18,3,0,1)
,(5,'''systmem_1''','varchar',-1,null,null,'source_system','varchar',-1,null,null,1,1)

,(6,'L_ID','varchar',-1,null,null,'l_id','varchar',-1,null,null,1,1)
,(6,'Area_PerSqFT','varchar',-1,null,null,'area_persqft','numeric',null,18,2,0,1)
,(6,'Monthly_Rent','varchar',-1,null,null,'monthly_rent','numeric',null,18,3,0,1)
,(6,'Lease_From','varchar',-1,null,null,'lease_from','date',null,null,null,1,1)
,(6,'Lease_To','varchar',-1,null,null,'lease_to','date',null,null,null,0,1)
,(6,'''systmem_1''','varchar',-1,null,null,'source_system','varchar',-1,null,null,1,1)


,(7,'tenant_uuid','varchar',-1,null,null,'tenant_uuid','varchar',100,null,null,1,1)
,(7,'tenant_id','varchar',-1,null,null,'tenant_id','varchar',100,null,null,0,1)
,(7,'lease_type','varchar',-1,null,null,'lease_type','varchar',100,null,null,0,1)
,(7,'lease_status','varchar',-1,null,null,'lease_status','varchar',50,null,null,0,1)
,(7,'category','varchar',-1,null,null,'category','varchar',100,null,null,0,1)
,(7,'source_system','varchar',-1,null,null,'source_system','varchar',20,null,null,0,1)


,(8,'tenant_uuid','varchar',-1,null,null,'tenant_uuid','varchar',100,null,null,1,1)
,(8,'area_persqft','numeric',null,18,3,'area_persqft','numeric',null,18,3,0,1)
,(8,'monthly_rent','numeric',null,18,3,'monthly_rent','numeric',null,18,3,0,1)
,(8,'annual_rent','numeric',null,18,3,'annual_rent','numeric',null,18,3,0,1)
,(8,'lease_from','date',null,null,null,'lease_from','date',null,null,null,1,1)
,(8,'lease_to','date',null,null,null,'lease_to','date',null,null,null,0,1)
,(8,'source_system','varchar',-1,null,null,'source_system','varchar',20,null,null,0,1)


,(9,'tenant_uuid','varchar',-1,null,null,'tenant_uuid','varchar',100,null,null,1,1)
,(9,'year','smallint',null,null,null,'year','smallint',null,null,null,1,1)
,(9,'month','smallint',null,null,null,'month','smallint',null,null,null,1,1)
,(9,'source_main','numeric',null,18,3,'source_main','numeric',null,18,3,0,1)
,(9,'source_sub','numeric',null,18,3,'source_sub','numeric',null,18,3,0,1)
,(9,'final_sales','numeric',null,18,3,'final_sales','numeric',null,18,3,0,1)
,(9,'source_system','varchar',-1,null,null,'source_system','varchar',20,null,null,0,1)




,(10,'tenant_uuid','varchar',-1,null,null,'tenant_uuid','varchar',100,null,null,1,1)
,(10,'area_persqft','numeric',null,18,3,'area_persqft','numeric',null,18,3,0,1)
,(10,'monthly_rent','numeric',null,18,3,'monthly_rent','numeric',null,18,3,0,1)
,(10,'annual_rent','numeric',null,18,3,'annual_rent','numeric',null,18,3,0,1)
,(10,'lease_from','date',null,null,null,'lease_from','date',null,null,null,1,1)
,(10,'lease_to','date',null,null,null,'lease_to','date',null,null,null,0,1)
,(10,'source_system','varchar',-1,null,null,'source_system','varchar',20,null,null,0,1)


,(11,'tenant_uuid','varchar',-1,null,null,'tenant_uuid','varchar',100,null,null,1,1)
,(11,'year','smallint',null,null,null,'year','smallint',null,null,null,1,1)
,(11,'month','smallint',null,null,null,'month','smallint',null,null,null,1,1)
,(11,'source_main','numeric',null,18,3,'source_main','numeric',null,18,3,0,1)
,(11,'source_sub','numeric',null,18,3,'source_sub','numeric',null,18,3,0,1)
,(11,'final_sales','numeric',null,18,3,'final_sales','numeric',null,18,3,0,1)
,(11,'source_system','varchar',-1,null,null,'source_system','varchar',20,null,null,0,1)

,(12,'tenant_uuid','varchar',-1,null,null,'tenant_uuid','varchar',100,null,null,1,1)
,(12,'tenant_id','varchar',-1,null,null,'tenant_id','varchar',100,null,null,0,1)
,(12,'lease_type','varchar',-1,null,null,'lease_type','varchar',100,null,null,0,1)
,(12,'lease_status','varchar',-1,null,null,'lease_status','varchar',50,null,null,0,1)
,(12,'category','varchar',-1,null,null,'category','varchar',100,null,null,0,1)
,(12,'source_system','varchar',-1,null,null,'source_system','varchar',20,null,null,0,1)

,(13,'Tenant_ID','varchar',100,null,null,'Tenant_ID','varchar',100,null,null,1,1)
,(13,'Lease_Type','varchar',100,null,null,'Lease_Type','varchar',100,null,null,0,1)
,(13,'Catagory','varchar',100,null,null,'Catagory','varchar',100,null,null,0,1)
,(13,'Area_Per_Sqft','numeric',null,18,3,'Area_Per_Sqft','numeric',null,18,3,0,1)
,(13,'Annual_Rent','numeric',null,18,3,'Annual_Rent','numeric',null,18,3,0,1)
,(13,'Lease_From','date',null,null,null,'Lease_From','date',null,null,null,0,1)
,(13,'Lease_To','date',null,null,null,'Lease_To','date',null,null,null,0,1)
,(13,'Annual_Sales','numeric',null,18,3,'Annual_Sales','numeric',null,18,3,0,1)

,(14,'Catagory','varchar',100,null,null,'Catagory','varchar',100,null,null,1,1)
,(14,'Rent_per_Category','numeric',null,18,3,'Rent_per_Category','numeric',null,18,3,0,1)
,(14,'Area_SqFt_per_Category','numeric',null,18,3,'Area_SqFt_per_Category','numeric',null,18,3,0,1)
,(14,'Sales_Per_Category','numeric',null,18,3,'Sales_Per_Category','numeric',null,18,3,0,1)



--- Create metadata.dbo.ELTLog table 
IF OBJECT_ID('metadata.dbo.ELTLog') IS NOT NULL
BEGIN 
	DROP TABLE metadata.dbo.ELTLog
END 
CREATE TABLE metadata.dbo.ELTLog (
	LogId int identity(1,1) NOT NULL,
	TaskId smallint,
	LineageID varchar(100),
	StartTime datetime,
	EndTime datetime,
	LogMessage varchar(max),
	InsertedDate datetime2 DEFAULT( GETDATE())
)
GO

SELECT * FROM metadata.dbo.Object
SELECT * FROM metadata.dbo.Task
SELECT * FROM metadata.dbo.TaskColumnMappping
SELECT * FROM metadata.dbo.ELTLog
