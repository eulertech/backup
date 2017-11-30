'''
Created on Mar 7, 2017

@author: VIU53188
@summary: This application will pull LiquidBalance data from Excel Spreadsheet and load it into RedShift
        1) take in the instance of ApplicationBase that sets up all the standard configurations
        2) pull file down from S3
        3) We are looking for two sheets {Crude, TightOil}
        4) for TightOil we can just load the entire sheet
            year is across the top and the category information {Region and Country} is located on the left side
            all data is in the remaining cells
        5) for Crude this still needs to be investigated more
        9) Generate CSV from values
        10) Load data into RedShift
'''

import os
import urllib
import urllib2
import json
from AACloudTools.S3Utilities import S3Utilities
from AACloudTools.RedshiftUtilities import RedshiftUtilities
from AACloudTools.FileUtilities import FileUtilities
from Applications.Common.ApplicationBase import ApplicationBase

class LiquidsBalance(ApplicationBase):
    '''
    This application will pull LiquidBalance data from Excel Spreadsheet and load it into RedShift
    '''
    def __init__(self):
        '''
        constructor
        '''
        super(LiquidsBalance, self).__init__()
        self.location = FileUtilities.PathToForwardSlash(os.path.dirname(os.path.abspath(__file__)))
        self.awsParams = ""
        self.lastFileName = None
        self.lastFileLocation = None

    def SetLastLiquidsBalanceFileInfo(self):
        '''
        no sure right now come back to this
        '''
        try:
            self.logger.info(self.moduleName + " - Calculating last liquids balance file uploaded in the share folder.")
            apiRequest = urllib2.Request(self.job["eaa_admin_services_url"] +\
                                        "/wow/GetLastLiquidBalanceFile?sharedFolder=" +\
                                        self.job["sharedSrcFolder"] +\
                                        "&fileExt=" +\
                                        self.job["inputFileExt"] + "&filePrefix=" + self.job["inputFilePrefix"])
            response = urllib2.urlopen(apiRequest)
            jResponse = json.load(response)

            self.lastFileName = jResponse["data"]["lastFileName"]
            self.lastFileLocation = jResponse["data"]["folder"]

            self.logger.info(self.moduleName + " - Last liquids balance file: " + self.lastFileName + " located in -> " + self.lastFileLocation)
        except Exception as err:
            self.logger.error(self.moduleName + " [moveFileFromSharedToS3] - Message: " + err.message)
            raise Exception(err.message)

    def ExtractSheet(self, sheetConfig):
        '''
        place holder
        '''
        try:
            self.logger.info(self.moduleName + " - Extracting data for sheet: " + sheetConfig["name"])
            sheetConfig = "[" + json.dumps(sheetConfig) + "]"
            sheetConfig = urllib.quote(sheetConfig)

            api = self.job["eaa_admin_services_url"] +\
                    "/excel/extract?fileName=" + self.lastFileName +\
                    "&srcLocation=" + self.lastFileLocation.replace("\\", "\\\\") +\
                    "&formatOut=" + self.job["sheetsOutputFormat"] +\
                    "&sheetsConfig=" + sheetConfig

            apiRequest = urllib2.Request(api)
            urllib2.urlopen(apiRequest)
        except Exception as err:
            self.logger.error(self.moduleName + " [ExtractSheet] - Error while trying to extract sheet data to S3...")
            raise Exception(err)

    def ProcessLiquidBalanceFile(self):
        '''
        place holder
        '''
        try:
            rsConnect = RedshiftUtilities.Connect(dbname=self.awsParams.redshift['Database'],
                                                  host=self.awsParams.redshift['Hostname'],
                                                  port=self.awsParams.redshift['Port'],
                                                  user=self.awsParams.redshiftCredential['Username'],
                                                  password=self.awsParams.redshiftCredential['Password'])

            for sheetConfig in self.job["sheetsToExtract"]:
                self.ExtractSheet(sheetConfig)

                s3key = self.job["s3SrcDirectory"] + "/" + sheetConfig["outputName"] + "." + self.job["sheetsOutputFormat"] + ".gz"
                self.logger.info(self.moduleName + " Uploading information to redshift for worksheet: " + sheetConfig["name"])

                job = {}
                job["destinationSchema"] = self.job["destinationSchema"]
                job["tableName"] = sheetConfig["tempTableName"]
                job["s3Filename"] = S3Utilities.GetS3FileName(self.job["bucketName"], s3key)
                job["fileFormat"] = self.job["fileFormat"]
                job["dateFormat"] = self.job["dateFormat"]
                job["delimiter"] = sheetConfig["delimiter"]

                RedshiftUtilities.LoadDataFromS3(rsConnect, self.awsParams.s3, job, self.logger)
                S3Utilities.DeleteFile(self.awsParams.s3, self.job["bucketName"], s3key)
        except:
            self.logger.exception(self.moduleName + " [ProcessLiquidBalanceFile] - We had an error in LiquidsBalance during processBlock")
            raise

    def Start(self, logger, moduleName, filelocs):
        '''
        main routine starts here
        '''
        try:
            ApplicationBase.Start(self, logger, moduleName, filelocs)

            self.moduleName = moduleName
            self.CreateTables(self.job["tempTablesScript"])
            self.SetLastLiquidsBalanceFileInfo()
            self.ProcessLiquidBalanceFile()
            self.CreateTables(self.job["unpivotScript"])
            self.CreateTables(self.job["cleanTempTablesScript"])
        except Exception as err:
            self.logger.error(self.moduleName + " - Exception in start.")
            raise err
