import sys
from datetime import datetime
sys.path.insert(0, './task')
from dbloadmain import DBLoadMain


class RawMain:
    def __init__(self,strDataLineageID) -> None:
        self.__strDataLineageID = strDataLineageID
    
    def loadRawZone(self):
        try:
            self.__loadSourceToLandingLayer()
        except Exception as e:
            raise e
    def __loadSourceToLandingLayer(self):
        #**************************** Load data from Source to Landing Layer ************************/
        try:
            strJobName = 'sourcetolanding'
            db = DBLoadMain(strJobName,self.__strDataLineageID)
            db.processData()
        except Exception as e:
            raise e