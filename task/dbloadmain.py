import sys
sys.path.insert(0, './utility')
from DBUtility import DBFunctions
from  dbloadprocessing import DBProcessing


class DBLoadMain:

    def __init__(self,strJobName,strDataLineageID) -> None:
        self.strJobName = strJobName
        self.strDataLineageId = strDataLineageID
        self.dbFunc = DBFunctions()
        
    def __getMetaDataQuery(self) -> str:
        try:
            #/****************** Get Metadata and process the tables. ****************/
            sqlQuery ="SELECT DISTINCT "\
            +" CONCAT(tar.[DataBaseName],'.',tar.[SchemaName],'.',tar.[ObjectName]) as TargetTableName"\
            +" FROM metadata.dbo.Task t"\
            +" INNER JOIN metadata.dbo.Object so"\
            +" ON so.ObjectId =t.SourceObjectID"\
            +" INNER JOIN metadata.dbo.Object tar"\
            +" ON tar.ObjectId=t.TargetObjectId"\
            +" INNER JOIN metadata.dbo.TaskColumnMappping tcm"\
            +" ON tcm.TaskId = t.TaskID"\
            +" WHERE lower(JobName)=LOWER('"+self.strJobName+"')"\
            +" and so.IsActive=1"\
            +" and tar.IsActive=1"\
            +" and t.IsActive=1"
            return sqlQuery
        except Exception as e:
            raise e
        
    def processData(self):
        try:
            strSql=self.__getMetaDataQuery()
            metaData = self.dbFunc.readSQLMetaData(strSql)
            for i, data in metaData.iterrows():
                dbprocess = DBProcessing(data['TargetTableName'],self.strJobName,self.strDataLineageId)    
                dbprocess.processTable()
        except Exception as e:
            raise e

    


