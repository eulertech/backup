'''
Created on Feb 13, 2017

@author: VIU53188
@summary: This application will pull ENP Montly data from Shooju and load it into RedShift
    Before you start make you have done a pip install shooju
        1)  take in the instance of ApplicationBase that sets up all the standard configurations
        2)  set up connection to shooju
        3)  pull data for specific links
        4)  create csv from data
        5)  load the CSV file into RedShift
'''

import os
import re
import csv
from datetime import date
import shooju

from AACloudTools.RedshiftUtilities import RedshiftUtilities
from AACloudTools.FileUtilities import FileUtilities
from AACloudTools.S3Utilities import S3Utilities
from Applications.Common.ApplicationBase import ApplicationBase

class ENPMonthly(ApplicationBase):
    '''
    This application will pull ENP Montly data from Shooju and load it into RedShift
    '''
    def __init__(self):
        '''
        Constructor
        '''
        super(ENPMonthly, self).__init__()
        self.location = FileUtilities.PathToForwardSlash(os.path.dirname(os.path.abspath(__file__)))
        self.s3subFolder = None

    class CategoryClass(object):# pylint: disable=too-few-public-methods
        '''
        structure used
        '''
        def __init__(self):
            self.category = None
            self.frequency = "M"
            self.source = None
            self.unit = None
            self.description = None

    def LoadData(self, folderName, tblJson):
        '''
        load the data from s3 into RedShift
        '''
        try:
            self.logger.debug(self.moduleName + " -- " + "LoadData" + " starting ")
            rsConnect = self.etlUtilities.GetAWSConnection(self.awsParams)

            s3folder = "s3://" + self.job["bucketName"] + self.job["s3GzipFolderBase"] +\
                        "/" + folderName

            RedshiftUtilities.LoadDataFromS3(rsConnect, self.awsParams.s3,
                                             {
                                                 "destinationSchema": self.job["destinationSchema"],
                                                 "tableName": tblJson["table"],
                                                 "s3Filename": s3folder,
                                                 "fileFormat": self.job["fileFormat"],
                                                 "dateFormat": self.job["dateFormat"],
                                                 "delimiter": self.job["delimiter"]
                                             },
                                             self.logger, "N")
            self.logger.info(self.moduleName + " - Finished loading s3 data to " +\
                             self.job["destinationSchema"] + '.' +  tblJson["table"])
            rsConnect.close()
            self.logger.debug(self.moduleName + " -- " + "LoadData" + " finished ")
        except:
            self.logger.exception(self.moduleName + " - we had an error in LoadData")
            raise

    def ProcessRequest(self):
        '''
        gets data from shooju and creates csv file
        '''
        try:
            self.logger.debug(self.moduleName + " -- " + "ProcessRequest starting ")
#            conn = shooju.Connection(server='ihs', user=self.job["per_svc_act"], api_key = self.job["per_apikey"])
            conn = shooju.Connection(server='ihs', user=self.job["svc_act"], api_key=self.job["apikey"])
#            suppFields = conn.get_fields(r'IHS\EI\MIS\Supply\Crude\Production\World')
            suppFields = conn.get_fields(self.job["supplypath"])
            cat = self.CategoryClass()
            cat.category = "Supply"
            cat.description = suppFields["description"]
            cat.source = suppFields["source"]
            cat.unit = suppFields["unit"]
            sdate = date(self.job["startdate"]["year"], \
                         self.job["startdate"]["month"],  \
                         self.job["startdate"]["day"])
####
#  max_points is set to an enormous number in the config file at the time of this run is 10000000000
#  date_start is currently set to 1/1/1990 but this may change depending on the process we create
####
            pts = conn.get_points(self.job["supplypath"], date_start=sdate, max_points=self.job["maxpoint"])
#            conn.get_points(series_id, date_start, date_finish, max_points, snapshot_job_id, snapshot_date, size)
##
##
#  take the data and convert it to CSV
##
            outputFileName = self.fileUtilities.csvFolder + '/ENP_Monthlydata.csv'
            csvfile = open(outputFileName, 'wb')
            csvWriter = csv.writer(csvfile)

###
#  load an array that will contain a class object that looks just like we need it for the CSV
###
            outRecArray = []
            for pt in pts:
                outRecArray = []
                outRecArray.append(cat.category)
                outRecArray.append(cat.frequency)
                outRecArray.append(cat.description)
                outRecArray.append(cat.source)
                outRecArray.append(cat.unit)
                outRecArray.append(pt.date)
                outRecArray.append(pt.value)
                csvWriter.writerow(outRecArray)

            self.logger.debug(self.moduleName + " -- " + "ProcessRequest starting ")
        except:
            self.logger.exception(self.moduleName + " - we had an error in ProcessRequest")
            raise
        finally:
            csvfile.close()
        return outputFileName

    def BulkUploadToS3(self, s3subFolder):
        '''
        Uploads all GZIP files created into S3 to be uploaded later...
        '''
        self.logger.info(self.moduleName + " - Uploading GZIP files to s3 folder...")
        s3Sub = None
        if s3subFolder is not None:
            s3Sub = '/' + s3subFolder
        S3Utilities.SyncFolderAWSCli(self.fileUtilities.gzipFolder,
                                     "s3://" + self.job["bucketName"] + self.job["s3GzipFolderBase"] + s3Sub,
                                     args='''--quiet --include "*.gz"''', dbug="Y")

    def CreateUpdateScript(self, pEtlSchema, pEtlTable, tblJson, currProcId):
        '''
        takes the template for the Update sqlscript and customizes it
        '''
        sqlUpdateScript = None
        try:
            self.logger.debug(self.moduleName + " -- " + "UpDate Table Script" + " starting ")
            sqlUpdateTemplate = self.location + '/sql/' + self.job["sqlUpdateScript"]
            sqlUpdateScript = self.localTempDirectory + "/sql/" + re.sub('Template.sql$', '.sql', self.job["sqlUpdateScript"])

            FileUtilities.RemoveFileIfItExists(sqlUpdateScript)
            ###
            #  gather variables needed
            ###

            tbworkingsourceName = None
            tdestinationName = None

            for table in tblJson:
                if "type" in table:
                    if table["type"] == "working":
                        tbworkingsourceName = table["table"]
                    elif table["type"] == "destination":
                        tdestinationName = table["table"]

            with open(sqlUpdateTemplate) as infile, open(sqlUpdateScript, 'w') as outfile:
                for line in infile:
                    line = line.replace('{schemaName}', self.job["destinationSchema"])
                    line = line.replace('{tbworkingsourceName}', tbworkingsourceName)
                    line = line.replace('{tdestinationName}', tdestinationName)
                    line = line.replace('{tbstats}', pEtlSchema + "." + pEtlTable)
                    line = line.replace('{procid}', str(currProcId))
                    outfile.write(line)
            self.logger.debug(self.moduleName + " -- " + "UpDate Table Script" + " finished ")
        except:
            self.logger.exception(self.moduleName + " - we had an error in UpDate Table Script")
            raise
        return sqlUpdateScript

    def UpdateTable(self, pEtlSchema, pEtlTable, tblJson, currProcId):
        '''
        Routine to create and run the sql to create version in RedShift
        '''
        try:
            self.logger.debug(self.moduleName + " -- " + "Update tables for " + pEtlSchema + "." + pEtlTable + " starting ")
            verScript = self.CreateUpdateScript(pEtlSchema, pEtlTable, tblJson, currProcId)
            self.logger.info(self.moduleName + " - script file name = " + verScript)
            RedshiftUtilities.PSqlExecute(verScript, self.logger)
            self.logger.debug(self.moduleName + " -- " + "Update tables for " + pEtlSchema + "." + pEtlTable + " finished ")
        except:
            self.logger.exception(self.moduleName + " - we had an error in UpdateTable")
            raise

    def Start(self, logger, moduleName, filelocs):
        '''
        Main starting routine
        '''
        currProcId = None
        try:
            ApplicationBase.Start(self, logger, moduleName, filelocs)
            self.logger.debug(self.moduleName + " -- " + " starting ")
            currProcId = self.etlUtilities.GetRunID(filelocs["tblEtl"]["table"], self.moduleName)
###
#  set up to run create folder
###
            self.fileUtilities.moduleName = self.moduleName
            self.fileUtilities.localBaseDirectory = self.localTempDirectory
            self.fileUtilities.CreateFolders(self.job["folders"])
###
            for tblJson in self.job["tables"]:
                fname = self.fileUtilities.CreateTableSql(tblJson, self.fileUtilities.sqlFolder)
                RedshiftUtilities.PSqlExecute(fname, self.logger)
                if "s3subfolder" in tblJson:
                    self.s3subFolder = tblJson["s3subfolder"]

            outputFileName = self.ProcessRequest()

            outputCSV = outputFileName
            outputGZ = self.fileUtilities.gzipFolder + self.moduleName + '.csv.gz'
            self.fileUtilities.GzipFile(outputCSV, outputGZ)
            self.BulkUploadToS3(self.s3subFolder)
            for tblJson in self.job["tables"]:
                if "s3subfolder" in tblJson:
                    self.LoadData(tblJson["s3subfolder"], tblJson)
            self.UpdateTable(filelocs["tblEtl"]["schemaName"], filelocs["tblEtl"]["table"], self.job["tables"], currProcId)
            if self.job["cleanlocal"] == "Y":
                self.fileUtilities.RemoveFolder(self.localTempDirectory)
            self.logger.debug(self.moduleName + " -- " + " finished ")
        except Exception as err:
            self.logger.exception(moduleName + " - Exception! Error: " + err.message)
            if self.etlUtilities.CompleteInstance(filelocs["tblEtl"]["table"],\
                                             currProcId, 'F') is not True:
                self.logger.info(self.moduleName + " - we could not Complete Instance.")
            raise Exception(err.message)
        