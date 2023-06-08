import sys
import os
from datetime import datetime
sys.path.insert(0, './task')
from dbloadmain import DBLoadMain
sys.path.insert(0, './utility')
from DBUtility import DBFunctions

class CuratedMain:
    def __init__(self,strDataLineageID) -> None:
        self.__strDataLineageID= strDataLineageID
        self.dbFunc = DBFunctions()
        
    def loadCuratedZone(self):
        try:
            self.__createSupporingViewAfterTableCreated('enrichment')
            self.__loadEnrichmentToDiscoveryLayer()
        except Exception as e:
            raise e
    #**************************** Load data from Enrichment to Discovery Layer ************************/
    # This data will be loaded one to one from Enrichment to Discovery. 
    # This will be our consumption layer for and AnlayticalTool (Tableau, PowerBI) etc.
    # Analytical Product like report and othre must have only access to DiscoveryLayer. 
    def __loadEnrichmentToDiscoveryLayer(self):
        try:    
            strJobName = 'enrichmenttodiscovery'
            db = DBLoadMain(strJobName,self.__strDataLineageID)
            db.processData()
        except Exception as e:
            raise e
    
    def __createSupporingViewAfterTableCreated(self,strLayerName):
        try:
            for f in os.listdir('./view'):
                if f.lower().startswith(strLayerName) :
                    strSql = "SELECT OBJECT_ID('"+os.path.splitext(f)[0]+"') AS ID"
                    df = self.dbFunc.readSQLMetaData(strSql,strDataBase='quality_zone')
                    if df['ID'][0] == None:
                        with open(os.path.join('./view',f),'r') as f:
                            strViewSql=f.read()
                            self.dbFunc.executeSqlQuery(strViewSql,strDataBase='quality_zone')
        except Exception as e:
            raise e