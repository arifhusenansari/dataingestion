import sys
import os
from datetime import datetime
sys.path.insert(0, './task')
from dbloadmain import DBLoadMain
sys.path.insert(0, './utility')
from DBUtility import DBFunctions

class QualityMain:

    def __init__(self,strDataLineageID) -> None:
        self.__strDataLineageID = strDataLineageID
        self.dbFunc = DBFunctions()

    def loalQualityZone (self):
        try:
            self.__loadLandingToCorrectionLayer()
            self.__createSupporingViewAfterTableCreated('correction')
            self.__loadCorrectionToEnrichmentLayer()
            self.__createSupporingViewAfterTableCreated('integration')
            self.__loadIntegrationToEnrichmentLayer()
        except Exception as e:
            raise e

    def __loadLandingToCorrectionLayer(self):
        #**************************** Load data from Landing to Correction Layer ************************/
        try:    
            strJobName = 'landingtocorrection'
            db = DBLoadMain(strJobName,self.__strDataLineageID)
            db.processData()
        except Exception as e:
            raise e
    def __loadCorrectionToEnrichmentLayer(self):
        #**************************** Load data from Correction to Enrichment Layer ****************************/
        # This is to load data in dimension table in Enrichment Layer
        try:
            
            strJobName = 'correctiontoenrichment'
            db = DBLoadMain(strJobName,self.__strDataLineageID)
            db.processData()
        except Exception as e:
            raise e
        
    def __loadIntegrationToEnrichmentLayer(self):
        #**************************** Load data from Integration to Enrichment Layer ****************************/
        # This is to load data in fact table in Enrichment Layer
        try:
            strJobName = 'integrationtoenrichment'
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
    