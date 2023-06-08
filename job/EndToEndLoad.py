from raw_main import RawMain
from quality_main import QualityMain
from curated_main import CuratedMain
from datetime import datetime

strDataLineageID=datetime.today().strftime('%Y%m%d%H%M%S')

#******************** Load Raw Zone *********************
rMain = RawMain(strDataLineageID)
rMain.loadRawZone()

#******************** Load Quality Zone *********************
qMain = QualityMain(strDataLineageID)
qMain.loalQualityZone()

#******************** Load Curated Zone *********************
cMain = CuratedMain(strDataLineageID)
cMain.loadCuratedZone()