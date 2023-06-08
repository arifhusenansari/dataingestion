import sys
from datetime import datetime
sys.path.insert(0, './task')
from dbloadmain import DBLoadMain
strDataLineageID=datetime.today().strftime('%Y%m%d%H%M%S')
# #**************************** Load data from Correction to Enrichment Layer ****************************/
# # This is to load data in dimension table in Enrichment Layer
# try:
    
#     strJobName = 'correctiontoenrichment'
#     db = DBLoadMain(strJobName,strDataLineageID)
#     db.processData()
# except Exception as e:
#     raise e

#**************************** Load data from Integration to Enrichment Layer ****************************/
# This is to load data in fact table in Enrichment Layer
try:
    
    strJobName = 'integrationtoenrichment'
    db = DBLoadMain(strJobName,strDataLineageID)
    db.processData()
except Exception as e:
    raise e