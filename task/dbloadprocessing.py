import pandas as pd
import sys
sys.path.insert(0, './utility')
from DBUtility import DBFunctions
from datetime import datetime

class DBProcessing:
    def __init__(self,strTargetTableName,strJobName,strDataLineageId) -> None:
        self.__strTargetTableName = strTargetTableName
        self.__strJobName = strJobName
        self.__strDataLineageId = strDataLineageId
        self.dbFunc = DBFunctions()
        self.auditColumns = pd.Series(['etl_inserted_date datetime','etl_lastmodified_date datetime','etl_lineage_id varchar(100)'])
        # self.auditColumnsDefaultValues = [datetime.today(),datetime.today(),self.__strDataLineageId]
        self.auditColumnsDefaultValues = [datetime.today().strftime('%Y-%m-%d %H:%M:%S'),datetime.today().strftime('%Y-%m-%d %H:%M:%S'),self.__strDataLineageId]
        self.strSplitTableDetails = self.__strTargetTableName.split('.')
        self.dateTimeStamp = datetime.today().strftime('%Y%m%d')
        self.strTempViewName = "{schemaName}.temp_{tableName}_{timeStamp}"\
                .format(schemaName =self.strSplitTableDetails[1],tableName=self.strSplitTableDetails[2],timeStamp=self.dateTimeStamp)

    def processTable(self):
        try:
            strSqlQuery = self.__getMetaDataQuery()
            tableMetadata=self.dbFunc.readSQLMetaData(strSqlQuery)
            self.__createSchema()
            self.__createTable(tableMetadata)
            self.__loadTargetTable(tableMetadata)
        except Exception as e:
            raise e
        
    def __createSchema(self):
        try:
            
            strSql = "USE "+self.strSplitTableDetails[0]\
            +"\nIF SCHEMA_ID('"+self.strSplitTableDetails[1]+"') IS NULL"\
            +"\n BEGIN"\
            +"\n    EXEC('CREATE SCHEMA "+self.strSplitTableDetails[1]+"')"\
            +"\n END"
            # print(strSql)
            self.dbFunc.executeSqlQuery(strSql)
        except Exception as e:
            raise e
        
    def __createTable(self,tableMetadata):
        try:
            strTargetColumnNames= self.__getColumnName(tableMetadata)
            strCreatTableScript = "IF OBJECT_ID('"+self.__strTargetTableName+"') IS NULL"\
            +"\n BEGIN"\
            +"\n CREATE TABLE "+self.__strTargetTableName+" ("\
            +"\n "+strTargetColumnNames\
            +"\n )"\
            +"\n END"
            # print(strCreatTableScript)
            self.dbFunc.executeSqlQuery(strCreatTableScript)
        except Exception as e:
            raise e
    
   
    def __loadTargetTable(self,tableMetadata):
        try:
            strSourceObjectType = tableMetadata['SourceObjectType'][0]
            loadStartTime = datetime.today()
            if strSourceObjectType.lower()=='file':
                strSourceFilePath = tableMetadata['SourceFilePath'][0]+"\\"+tableMetadata['SourceTableName'][0]+"."+tableMetadata['SourceExt'][0]
                strSourceFileExt = tableMetadata['SourceExt'][0]
                strColumDelim = tableMetadata['SouceColumDelim'][0]
                strTextQualifier = tableMetadata['SourceTextQualifier'][0]
                if strSourceFileExt == 'csv':
                    colMapping = self.__getColumnMappingforFile(tableMetadata)
                    data = pd.read_csv(strSourceFilePath,sep=strColumDelim,quotechar=strTextQualifier,keep_default_na=False)
                    data.rename(columns=colMapping,inplace=True)
                    for i,colName in self.auditColumns.items():
                        colNameSplit = colName.split(' ')[0]
                        data[colNameSplit] = self.auditColumnsDefaultValues[i]
                    self.dbFunc.insertDataIntoTable(data,self.__strTargetTableName)
            elif strSourceObjectType.lower() in ('table','view'):
                self.__createIncrementalViewQuery(tableMetadata)
                self.__createMergeQuery(tableMetadata)
                self.__dropIncrementalView()

            loadEndTime = datetime.today()
            self.__loadELTLogTable(loadStartTime,loadEndTime,tableMetadata)
        except Exception as e:
            raise e
    
    def __loadELTLogTable(self,loadStartTime,loadEndTime,tableMetadata):
        try:
            logData={
                        'TaskID':tableMetadata['TaskID'][0]
                        ,'LineageID':self.__strDataLineageId
                        ,'StartTime':loadStartTime
                        ,'EndTime':loadEndTime
                        ,'LogMessage':"Sucessfull: Data is loaded from "+tableMetadata['SourceTableName'][0]+" into "+self.__strTargetTableName+"."
            }
            eltLogDf = pd.DataFrame(data=logData,index= range(0,1))
            self.dbFunc.insertDataIntoTable(eltLogDf,'metadata.dbo.ELTLog')
            
        except Exception as e:
            raise e
    def __getColumnMappingforFile(self,tableMetadata):
        try:
            dictColumnMapping ={}
            for i,data in tableMetadata.iterrows():
                dictColumnMapping[data['SourceColumnName']]=data['TargetColumnName']
            return dictColumnMapping    
        except Exception as e:
            raise e
        
    def __getColumnMappingforTable(self,tableMetadata):
        try:
            strSourceTargetMapping = "\n ,".join((tableMetadata['SourceColumnNameWithCast']) + ' AS '+ (tableMetadata['TargetColumnName']) )
            
            return strSourceTargetMapping
        except Exception as e:
            raise e
        
    def __createIncrementalViewQuery(self,tableMetadata):
        try:
                        
            strSourceTablename = "{dbName}.{schemaName}.{tableName}"\
                .format(dbName= tableMetadata['SourceDatabaseName'][0],schemaName = tableMetadata['SourceSchemaName'][0],tableName = tableMetadata['SourceTableName'][0])
            dfPrimaryKey = tableMetadata[tableMetadata['IsPrimary']=='Yes']
            strSourceTargetMapping = self.__getColumnMappingforTable(tableMetadata)
            
            strTempViewSQL = "CREATE VIEW "+self.strTempViewName+" AS SELECT * FROM ("
            strInnerQuery= "\n SELECT " + strSourceTargetMapping 
            if len(dfPrimaryKey) > 0:
                strPartitionBy = "\n ,ROW_NUMBER() OVER (PARTITION BY "
                strPartitionBy +=" ,".join(dfPrimaryKey['SourceColumnName'])
                strPartitionBy +=" ORDER BY etl_lastmodified_date DESC) AS RN FROM "+strSourceTablename+") AS Main WHERE RN=1"
            elif len(dfPrimaryKey)==0:
                strPartitionBy = "\n FROM "+strSourceTablename+" ) AS Main"
            strInnerQuery += strPartitionBy
            strTempViewSQL = strTempViewSQL+strInnerQuery 
            print(strTempViewSQL)
            self.dbFunc.executeSqlQuery(strTempViewSQL,self.strSplitTableDetails[0])
        except Exception as e:
            raise e
    def __dropIncrementalView (self):
        try:
            strQuery = "DROP VIEW {viewName}".format(viewName=self.strTempViewName)
            self.dbFunc.executeSqlQuery(strQuery,self.strSplitTableDetails[0])
        except Exception as e:
            raise e
    def __createMergeQuery(self,tableMetadata):
        try:
            strMergeQuery= "MERGE INTO "+tableMetadata['TargetSchemaName'][0]+"."+tableMetadata['TargetTableName'][0]+" AS target"\
            +"\n USING "+self.strTempViewName+ " AS source"
            strMergeJoinCondition ="\n ON "
            dfPrimaryKey = tableMetadata[tableMetadata['IsPrimary']=='Yes']
            dfUpdateColumList = tableMetadata[tableMetadata['IsPrimary']=='No']
            if len(dfPrimaryKey) > 0:
                strMergeJoinCondition += "\n AND ".join("target."+ dfPrimaryKey['TargetColumnName'] + ' = '+ "source." + dfPrimaryKey['TargetColumnName'] )
            strInsertColumList="\n ,".join(tableMetadata['TargetColumnName'])
            strInsertColumnValue = "\n ,".join("source."+tableMetadata['TargetColumnName'])
            strUpdateColumnList = "\n ,".join("target."+ dfUpdateColumList['TargetColumnName'] + ' = '+ "source." + dfUpdateColumList['SourceColumnName'] )
            for i,colName in self.auditColumns.items():
                        strAuditColumnName = colName.split(' ')[0]
                        strInsertColumList += " ," + strAuditColumnName
                        strInsertColumnValue += " ,'"+self.auditColumnsDefaultValues[i]+"'"
                        if strAuditColumnName != "etl_inserted_date":
                            strUpdateColumnList += " ,target."+colName.split(' ')[0] +" = '"+self.auditColumnsDefaultValues[i]+"'"
            strMergeQuery += strMergeJoinCondition\
                +"\n WHEN MATCHED THEN UPDATE SET "\
                +"\n "+strUpdateColumnList \
                +"\n WHEN NOT MATCHED THEN "\
                +"\n INSERT("+strInsertColumList +")"\
                +"\n VALUES("+strInsertColumnValue+")"\
                +";"
            print(strMergeQuery)
            self.dbFunc.executeSqlQuery(strMergeQuery,strDataBase=tableMetadata['TargetDatabaseName'][0])
        except Exception as e:
            raise e

    def __getMetaDataQuery(self) -> str:
        try:
            #/****************** Get Metadata and process the tables. ****************/
            sqlQuery ="SELECT "\
                    +"t.[TaskID],t.[JobName],so.[ObjectName] as SourceTableName,so.[DataBaseName] as SourceDatabaseName"\
                    +" ,so.[SchemaName] as SourceSchemaName,so.[ObjectType] as SourceObjectType,so.[SourcePath] as SourceFilePath"\
                    +" ,so.[ProcessedPath] as SourceProcessedPath,so.[ErrorPath] as SourceErrorPath,so.[Extension] as SourceExt"\
                    +" ,so.[ColumnDelim] as SouceColumDelim,so.[RowDelim] as SourceRowDelim,so.[TextQualifire] as SourceTextQualifier"\
                    +" ,tcm.SourceColumnName as SourceColumnName"\
                    +" ,CASE  WHEN tcm.TargetColumnType in ('int','smallint','numeric','decimal','float') or tcm.TargetColumnType like '%date%' THEN 'TRY_CAST(' + tcm.SourceColumnName + ' AS '+ CONCAT(tcm.TargetColumnType, CASE WHEN tcm.TargetColumnType IN ('varchar','nvarchar') THEN CASE WHEN  tcm.TargetColumnLength = -1 THEN '(max)' ELSE concat('(',tcm.TargetColumnLength,')') END  WHEN tcm.TargetColumnType IN ('numeric','decimal') THEN concat('(',tcm.TargetColumnPrecision,',',tcm.TargetColumnScale,')') ELSE '' END) +')'  ELSE tcm.SourceColumnName END SourceColumnNameWithCast "\
                    +" ,CONCAT(tcm.SourceColumnType,CASE WHEN tcm.SourceColumnType IN ('varchar','nvarchar') THEN CASE WHEN  tcm.SourceColumnLength = -1 THEN '(max)' ELSE concat('(',tcm.SourceColumnLength,')') END "\
                    +" WHEN tcm.SourceColumnType IN ('numeric','decimal') THEN concat('(',tcm.SourceColumnPrecision,',',tcm.SourceColumnScale,')')"\
					+" ELSE '' END) as SourceColumnType"\
                    +" ,tar.[ObjectName] as TargetTableName"\
                    +" ,tar.[DataBaseName] as TargetDatabaseName"\
                    +" ,tar.[SchemaName] as TargetSchemaName"\
                    +" ,tar.[ObjectType] as TargetObjectType"\
                    +" ,tcm.TargetColumnName as TargetColumnName"\
                    +" ,CONCAT(tcm.TargetColumnType, CASE WHEN tcm.TargetColumnType IN ('varchar','nvarchar') THEN CASE WHEN  tcm.TargetColumnLength = -1 THEN '(max)' ELSE concat('(',tcm.TargetColumnLength,')') END "\
                    +" WHEN tcm.TargetColumnType IN ('numeric','decimal') THEN concat('(',tcm.TargetColumnPrecision,',',tcm.TargetColumnScale,')')"\
					+" ELSE '' END) as TargetColumnType"\
                    +" ,CASE tcm.IsPrimary WHEN 1 then 'Yes' else 'No' end as IsPrimary"\
                    +" FROM metadata.dbo.Task t"\
                    +" INNER JOIN metadata.dbo.Object so"\
                    +" ON so.ObjectId =t.SourceObjectID"\
                    +" INNER JOIN metadata.dbo.Object tar"\
                    +" ON tar.ObjectId=t.TargetObjectId"\
                    +" INNER JOIN metadata.dbo.TaskColumnMappping tcm"\
                    +" ON tcm.TaskId = t.TaskID"\
                    +" WHERE lower(JobName)=LOWER('"+self.__strJobName+"')"\
                    +" AND LOWER(CONCAT(tar.[DataBaseName],'.',tar.[SchemaName],'.',tar.[ObjectName])) = LOWER('"+self.__strTargetTableName+"')"\
                    +" and so.IsActive=1"\
                    +" and tar.IsActive=1"\
                    +" and t.IsActive=1"
            return sqlQuery
        except Exception as e:
            raise e
    def __getColumnName (self,tableMetadata):
        try:
            targetTableColumns = pd.Series()
            targetTableColumns = tableMetadata['TargetColumnName'].str.cat(tableMetadata['TargetColumnType'],sep=' ')
            targetTableColumns = targetTableColumns._append(self.auditColumns)
            strTargetColumnNames = "\n,".join(list(targetTableColumns))
            return strTargetColumnNames
        except Exception as e:
            raise e
        