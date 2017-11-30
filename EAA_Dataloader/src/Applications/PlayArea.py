'''
Created on Jan 20, 2017

@author: VIU53188
'''
import os
import sys
import datetime

import csv
import pypyodbc

from AACloudTools.FileUtilities import FileUtilities
from AACloudTools.S3Utilities import S3Utilities
from AACloudTools.ExcelUtilities import ExcelUtilities
from Applications.Common.ApplicationBase import ApplicationBase
# pylint: disable=unused-variable
#from Applications.Magellan.MagellanUtilities import MagellanUtilities
from Applications.GEForecast.GEForecastHistory import GEForecastHistory

class PlayArea(ApplicationBase):
    '''
    classdocs
    '''
    def __init__(self):
        '''
        Constructor
        '''
        super(PlayArea, self).__init__()
        self.location = FileUtilities.PathToForwardSlash(os.path.dirname(os.path.abspath(__file__)))
        
    def TestZipIt(self):
        '''
        routine to just gzip file
        '''
        try:
            localFilepath = "C:\\WorkSpaceEclipse36\\EAA_Dataloader_Data\\input\\Play\\COTHist2011.csv"
            zipLocalFilepath = localFilepath + ".gz"
            self.fileUtilities.GzipFile(localFilepath, zipLocalFilepath)
        except:
            raise
        
    def MoveFolderToS3(self):
        '''
        '''
        bucketName = "ihs-temp"
        s3GzipFolderBase = "/viu53188"
        s3subfolder = "EHSA"
        s3Location = "s3://" + bucketName + s3GzipFolderBase + "/" +\
                    "test/" + s3subfolder
        localFilepath = "C:\\WorkSpaceEclipse36\\EAA_Dataloader_Data\\input\\Play\\gzip"
        S3Utilities.CopyItemsAWSCli(localFilepath,
                                    s3Location,
                                    "--recursive --quiet")        
    def TestGEForecastHistory(self):
        '''
        bootstrap for testing GEForecast
        '''
        try:

            cat = {
                "name": "giif",
                "tables": [
                    {
                        "schemaName": "eaa_dev",
                        "srctable": "GEForcast_giif_attributes_working",
                        "table": "GEForcastHistory_giif_attributes",
                        "new": "Y",
                        "type": "attributes",
                        "partition": {
                            "over": "mnemonic, frequencychar",
                            "order": "publisheddate"
                        },
                        "fields": [
                            {"name": "object_id", "type": "VARCHAR", "size": "30"},
                            {"name": "name", "type": "VARCHAR", "size": "100"},
                            {"name": "mnemonic", "type": "VARCHAR", "size": "100"},
                            {"name": "frequencyChar", "type": "VARCHAR", "size": "2"},
                            {"name": "geo", "type": "VARCHAR", "size": "20"},
                            {"name": "startDate", "type": "Date"},
                            {"name": "endDate", "type": "Date"},
                            {"name": "updatedDate", "type": "Date"},
                            {"name": "publishedDate", "type": "Date"},
                            {"name": "longLabel", "type": "VARCHAR", "size": "2000"},
                            {"name": "dataEdge", "type": "VARCHAR", "size": "100"}
                        ],
                        "sortkey":"object_id"
                    },
                    {
                        "schemaName": "eaa_dev",
                        "srctable": "GEForcast_giif_data_working",
                        "table": "GEForcastHistory_giif_data",
                        "new": "Y",
                        "type": "series",
                        "fields": [
                            {"name": "object_id", "type": "VARCHAR", "size": "30"},
                            {"name": "date", "type": "DATE"},
                            {"name": "value", "type": "FLOAT8"}
                        ],
                        "sortkey":"object_id, date"

                    }
                ]
            }

            commonParams = {}
            commonParams["cat"] = cat
            commonParams["moduleName"] = self.moduleName
            commonParams["loggerParams"] = "log"
            commonParams["sqlFolder"] = '''c:/temp/sql/'''

            gefh = GEForecastHistory()
            gefh.commonParams = commonParams
            gefh.fileUtilities = self.fileUtilities
            gefh.CreateTables()
            rsConnect = self.etlUtilities.GetAWSConnection(self.awsParams)
            recCount = gefh.GetRecordCount(rsConnect)
            print('number of records = ' + str(recCount))
            numIterations = gefh.GetNumberOfIterations(rsConnect)
            print("number of iterations = " + str(numIterations))
            rsConnect.close()
            gefh.MigrateData()

        except:
            raise

    @staticmethod
    def OutputCol(col):
        '''
        make sure we can report the correct values
        '''
        retVal = ''
        try:
            if col:
                if sys.version[0] == '3':
                    if isinstance(col, str):
                        retVal = col.encode('utf-8')
                    elif isinstance(col, float):
                        if (col > int(col)) - (col < int(col)) == 0:
                            retVal = int(col)
                        else:
                            retVal = col
                    else:
                        retVal = col
                elif sys.version[0] == '2':
                    if isinstance(col, unicode):
                        retVal = col.encode('utf-8')
                    elif isinstance(col, float):
                        if cmp(col, int(col)) == 0:
                            retVal = int(col)
                        else:
                            retVal = col
                    else:
                        retVal = col
        except Exception as ex:
            pass                        
        return retVal

    def TestConnectionSQLServer(self):
        '''
        test routine to see if we can connect to Maritime
        '''
        try:
            recNdx = 1
            outputfileName = 'C:/tmp' + '/play.csv'
            pypyodbc.lowercase = False
            driver = '{ODBC Driver 11 for SQL Server}'
            server = 'Columbia3.ihs.internal.corp'
            db = 'GTA_WAREHOUSE'
            connstr = 'DRIVER=%s;SERVER=%s;DATABASE=%s;Trusted_Connection=yes;' % (driver, server, db)
            conn = pypyodbc.connect(connstr)

            cursor = conn.cursor()
            sql = 'select top 100 * from EHSA'
            cursor.execute(sql)
            row = cursor.fetchone()

            if sys.version[0] == '3':
                with open(outputfileName, 'w', newline = '') as fl:
                    cFile = csv.writer(fl, delimiter=',')
                    while True:
                        if row is None:
                            break
                        t = list(map(self.OutputCol, row))
                        cFile.writerow(t)
                        row = cursor.fetchone()
                        recNdx += 1
            else:
                with open(outputfileName, 'wb',) as fl:
                    cFile = csv.writer(fl, delimiter=',')
                    while True:
                        if row is None:
                            break
                        t = list(map(self.OutputCol, row))
                        cFile.writerow(t)
                        row = cursor.fetchone()
                        recNdx += 1
        except Exception as ex:
            raise
    def ExcelTest(self):
        '''
        routine to test excel helper  ExcelUtilities
        '''
        try:
            excelUtilities = ExcelUtilities(self.logger)
            excelUtilities.Excel2CSV(r'C:\WorkSpaceEclipse36\EAA_Dataloader_Data\Input\Play\TestExcel.xlsx',\
                                     'Query',\
                                     r'C:\WorkSpaceEclipse36\EAA_Dataloader_Data\Input\Play/TCC_TEST.CSV',\
                                     r'C:\WorkSpaceEclipse36\EAA_Dataloader_Data\Input\Play',\
                                     defDateFormat='%m/%d/%Y %H:%M:%S')
        except Exception as ex:
            raise
        
    def TestDates(self, logger, moduleName):
        '''
        routine to test datetime routine
        '''
        try:
            maxdate = None
            maxdate = datetime.datetime.strftime(datetime.date.today(), '%Y-%m-%d')
            logger.info(moduleName + "- max date before = " + str(maxdate))
            maxdate = datetime.datetime.strftime(datetime.date.today()-\
                                        datetime.timedelta(days=1), '%Y-%m-%d')
            logger.info(moduleName + "- max date after = " + str(maxdate))
        except Exception as ex:
            raise
        
    def TestShooju(self, logger, moduleName):
        '''
        routine to test shooju methods
        '''
        from shooju import Connection

        try:
            logger.info(moduleName + "TestShooju started ")
            supplypath = "IHS\\EI\\MIS\\Supply\\Crude\\Production\\World"
            conn = Connection(server='ihs', user="api.aa.eea", api_key="K58l2UR5RCrTVUBeEOrftQpTNl9qJSyVhnSTBLiYYFoGw1uxyt")
            suppFields = conn.get_fields(supplypath)
            pts = conn.get_points("IHS\\EI\\MIS\\Supply\\Crude\\Production\\World", date_start=datetime.datetime.now(), size=1)
            logger.info(moduleName + "TestShooju finished ")
        except Exception as ex:
            raise

    def Start(self, logger, moduleName, filelocs):
        '''
        start it
        '''
        try:
            ApplicationBase.Start(self, logger, moduleName, filelocs)
            logger.exception(moduleName + "- starting play area")
#            self.TestZipIt()
#            self.MoveFolderToS3()
#            self.TestConnectionSQLServer()
####
#  nugget is wrong since latest changes
####
#            self.TestGEForecastHistory()
#            self.ExcelTest()
#            self.TestDates(logger, moduleName)
            self.TestShooju(logger, moduleName)
            logger.exception(moduleName + "- ending play area")
        except Exception as ex:
            logger.exception(moduleName + " - Exception!")
            raise
