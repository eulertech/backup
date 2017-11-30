import sys
import multiprocessing
from Applications.Magellan.MagellanUtilities import MagellanUtilities

print(multiprocessing.cpu_count())
print(sys.version_info)

zipFolder = "/fastdrive/EAA_Dataloader_Data/output/Magellan/zips/"
localTempDirectory = "/fastdrive/EAA_Dataloader_Data/output/Magellan"
attrFields = [
        {'name': 'source_id', 'type': 'VARCHAR', 'size': '20'}, 
        {'name': 'dri_mnemonic', 'type': 'VARCHAR', 'size': '30'}, 
        {'name': 'start_date', 'type': 'VARCHAR', 'size': '10'}, 
        {'name': 'end_date', 'type': 'VARCHAR', 'size': '10'}, 
        {'name': 'base_period_value', 'type': 'VARCHAR', 'size': '20'}, 
        {'name': 'short_label', 'type': 'VARCHAR', 'size': '500'}, 
        {'name': 'long_label', 'type': 'VARCHAR', 'size': '1000'}, 
        {'name': 'explorer_label', 'type': 'VARCHAR', 'size': '1000'}, 
        {'name': 'last_update_date', 'type': 'VARCHAR', 'size': '10'}, 
        {'name': 'document_type', 'type': 'VARCHAR', 'size': '20'}, 
        {'name': 'wefa_mnemonic', 'type': 'VARCHAR', 'size': '20'}
        ]
csvFolder = "/fastdrive/EAA_Dataloader_Data/output/Magellan/csv/"

####
#  make sure that  you have two folders already in the same location as your csv folder
#  attributes
#  data
###

commonParams = {}
commonParams["moduleName"] = "Magellan"
commonParams["zipFolder"] = zipFolder
commonParams["localTempDirectory"] = localTempDirectory
commonParams["loggerParams"] = "log"
commonParams["attrFields"] = attrFields
commonParams["csvFolder"] = csvFolder

try:
    mu = MagellanUtilities()
    mu.commonParams = commonParams
    mu.fl = sys.argv[1]
    mu.startHere()
except: 
    raise
