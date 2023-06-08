import pyodbc
import pandas as pd
class DBFunctions:

    def __init__(self) -> None:
        self.driverName ='SQL Server'
        self.sqlServerName ='DESKTOP-U578AVE\MSSQLSERVER2019'
        self.userName ='sa'
        self.userPassword ='Sa@123'
        
    def __getConnectionString(self,strDataBase):
        try:
            strdbConnectionString =""
            if strDataBase != "":
                strdbConnectionString ="DRIVER={0}; Server={1};Database={2};UID={3}; PWD={4}"\
                .format(self.driverName,self.sqlServerName,strDataBase,self.userName,self.userPassword)
            elif strDataBase == "":
                strdbConnectionString ="DRIVER={0}; Server={1};UID={2}; PWD={3}"\
                .format(self.driverName,self.sqlServerName,self.userName,self.userPassword)
            return strdbConnectionString
        except Exception as e:
            raise e
    def getDBConnection(self,strDataBase):
        strConnString = self.__getConnectionString(strDataBase)
        try:
            conn = pyodbc.connect(strConnString)
            return conn
        except Exception as e:
            raise e

    def readSQLMetaData(self,query,strDataBase=""):
        try:
            conn = self.getDBConnection(strDataBase)
            df = pd.read_sql_query(query,conn)
            return df
        except Exception as e:
            raise e
        finally:
            conn.close()

    def executeSqlQuery(self,query,strDataBase=""):
        try:
            conn = self.getDBConnection(strDataBase)
            conn.execute(query)
            conn.commit()
        except Exception as e:
            raise e
        finally:
            conn.close()
    def insertDataIntoTable (self,dataFrame,strTableName,strDataBase=""):
        try:
            strTargetColumnName =",".join(dataFrame.columns)
            strValues = ",".join('?' for i in enumerate(dataFrame.columns))
            conn = self.getDBConnection(strDataBase)
            cursor = conn.cursor()
            for row_count in range(0, dataFrame.shape[0]):
                chunkOfData = dataFrame.iloc[row_count:row_count + 1,:].values.tolist()
                tupleOfData = tuple(tuple(x) for x in chunkOfData)
                strInsertScript = "INSERT INTO "+strTableName + " ("+strTargetColumnName+") VALUES ("+strValues+")"
                cursor.executemany(strInsertScript,tupleOfData)    
            cursor.commit()
        except Exception as e:
            raise e
        finally:
            cursor.close()
            conn.close()
    

    



