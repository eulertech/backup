'''
Created on May 11, 2017
@author: VIU53188
@summary: This application will pull Magellan data from the MarkLogic system AKA ODATA and is used for incremental updates
    Below are the steps
    1)  create folders
    2)  create sql files for tables
    3)  build tables
    4  pull data
    5)  Create csv file from content
    6)  Load CSV files onto S3
    7)  load data from S3 into Redshift
'''
import os
import json
import re
import datetime
import time

import requests

from Applications.Common.ApplicationBase import ApplicationBase
from Applications.Magellan.MagellanUtilities import MagellanUtilities

from AACloudTools.RedshiftUtilities import RedshiftUtilities
from AACloudTools.FileUtilities import FileUtilities
from AACloudTools.S3Utilities import S3Utilities

class MagellanODATA(ApplicationBase):
    '''
    This application will pull Magellan data from the MarkLogic system AKA ODATA
    '''
    # pylint: disable=too-many-instance-attributes

    def __init__(self):
        '''
        Constructor
        '''
        super(MagellanODATA, self).__init__()
        self.location = FileUtilities.PathToForwardSlash(os.path.dirname(os.path.abspath(__file__)))
        self.attrTable = None
        self.dataTable = None
        self.attrFields = None
        self.dataFields = None
        self.filter = None

    def GatherFields(self, tables):
        '''
        gets a list of all the fields that will be used later
        '''
        try:
            self.logger.debug(self.moduleName + " -- " + "GatherFields" + " starting ")
            for tblName in tables:
                if "destName" in tblName:
                    if tblName["type"] == "attributes":
                        self.attrFields = tblName["fields"]
                        self.attrTable = tblName["table"]
                    elif tblName["type"] == "series":
                        self.dataFields = tblName["fields"]
                        self.dataTable = tblName["table"]
                    else:
                        self.logger.info("table fields not defined")
            self.logger.debug(self.moduleName + " -- " + "GatherFields" + " finished ")
        except:
            self.logger.exception(self.moduleName + "- we had an error in GatherFields")
            raise

    def GetFromDate(self, schemaName, tableName, dField):
        '''
        get the from date so that we know the last time the data was updated
        '''
        ###
        #  find the atrributes table so we know the name used
        ###
        self.logger.debug(self.moduleName + " -- " + "GetFromDate" + " starting ")

        fromDate = None
        ###
        #  first get a cursor to RedShift and then call to find the last update date
        ###
        rsConnect = self.etlUtilities.GetAWSConnection(self.awsParams)
        cur = rsConnect.cursor()
        try:
            command = '''select max(''' + dField + ''') as fromdate from ''' + \
                        schemaName + '.' + tableName
            cur.execute(command)
            data = cur.fetchall()
            fromDate = data[0][0]
        except:
            rsConnect.rollback()
            self.logger.exception(self.moduleName + "- we had an error in GetFromDate")
            raise
        finally:
            cur.close()
            rsConnect.close()
        self.logger.debug(self.moduleName + " -- " + "GetFromDate" + " finished ")
        return fromDate

    def CreateFilter(self, proc, paramsList):
        '''
        populate the filter to use on the url calls
        '''
        try:
            self.logger.debug(self.moduleName + " -- " + "CreateFilter" + " starting ")
            currDate = None
            lensparamsList = len(paramsList)
            if lensparamsList > 0:
                if "lastrun" in paramsList:
                    currDate = paramsList["lastrun"]
            else:
                for table in proc["tables"]:
                    if "type" in table:
                        if table["type"] == 'attributes':
                            currDate = self.GetFromDate(table["schemaName"], table["destName"], "last_update_date")
                            break
###
#  no previous run so use yesterdays date
###
            if currDate is None:
                currDate = datetime.datetime.strftime(datetime.date.today() - datetime.timedelta(days=1), '%Y-%m-%d')
                toDate = datetime.datetime.strptime(currDate, '%Y-%m-%d')
            else:
                ###
                #  bump date by one day
                ###
                toDate = datetime.datetime.strptime(currDate, '%Y-%m-%d') +\
                         datetime.timedelta(days=self.job["daysback"])

            yesterday = datetime.datetime.strptime(\
                                                   datetime.datetime.strftime(
                                                       datetime.date.today() - datetime.timedelta(days=1),
                                                       '%Y-%m-%d'),
                                                   '%Y-%m-%d')

            if toDate > yesterday:

#            if toDate > datetime.datetime.strftime(datetime.date.today()-datetime.timedelta(days=1), '%Y-%m-%d'):
                toDate = yesterday
###
#  calculate the first day
###
            fromDate = toDate - datetime.timedelta(days=self.job["daysback"]-1)
            fromDate = fromDate.strftime('%Y-%m-%d')
            toDate = toDate.strftime('%Y-%m-%d')

            fltr = "?$filter=last_update ge {}T00:00Z and last_update le {}T23:59Z".format(fromDate, toDate)
            fltr = fltr + "&document_type eq 'Timeseries'"
            self.logger.debug(self.moduleName + " -- " + "Filter = " + fltr)
            self.logger.debug(self.moduleName + " -- " + "CreateFilter" + " finished ")
            return fltr, toDate
        except:
            self.logger.exception(self.moduleName + "- we had an error in CreateFilter")
            raise

    def GetRecordCount(self, proc):
        '''
        Find out how many records are stored in the set
        '''
        retVal = 0
        try:
            self.logger.debug(self.moduleName + " -- " + "GetRecordCount " + proc["name"] + " starting ")
            ###
            #  create url to use to get a count of the complete set of data
            ###
            url = proc["endpoint"] + proc["name"]  + '/$count' + self.filter
            req = requests.get(url)
            if req.status_code == 200:
                retVal = req.text

            self.logger.debug(self.moduleName + " -- " + "GetRecordCount " + proc["name"] + " finished ")
        except:
            self.logger.exception(self.moduleName + "- we had an error in GetRecordCount - " + proc["name"])
            raise
        return retVal

    def PullOdataObj(self, proc, fromDate):
        '''
        The method to actually pull the data from odata
        '''
        self.logger.debug(self.moduleName + " -- " + "PullOdataObj " + proc["skip"] + " starting ")
        requestReturned = False
        url = proc["baseurl"] + proc["skip"]
        self.logger.debug(self.moduleName + " -- " + "url " + url)
        reqAttemptCount = 0
        while not requestReturned:
            try:
                req = requests.get(url)
                if req.status_code == 200:
                    data = json.loads(req.text)
                    dataLen = len(data["value"])
                    if dataLen > 0:
                        commonParams = {}
                        commonParams["moduleName"] = self.moduleName
                        commonParams["loggerParams"] = "log"
                        commonParams["attrFields"] = self.attrFields
                        commonParams["csvFolder"] = self.fileUtilities.csvFolder
                        commonParams["gzipFolder"] = self.fileUtilities.gzipFolder
                        mu = MagellanUtilities()
                        mu.logger = self.logger
                        mu.commonParams = commonParams
                        mu.fileUtilities = self.fileUtilities
                        mu.ProcessJson(data, fromDate + "_" + proc["skip"])
                        mu.GZipItUp(fromDate + "_" + proc["skip"])
                    else:
                        self.logger.exception(self.moduleName + "- completed PullOdataObj- no data for segment " + proc["skip"])
                    requestReturned = True
                else:
                    self.logger.exception(self.moduleName + "- completed PullOdataObj- " + proc["skip"] + " Error =  " + str(req.status_code))
                    requestReturned = True
            except:
                reqAttemptCount = reqAttemptCount + 1
                if reqAttemptCount > self.job["maxretries"]:
                    self.logger.debug(self.moduleName + " -- " + "PullOdataObj " + proc["skip"] + " exceeded max retries attempt " + str(self.job["maxretries"]))
                    requestReturned = True
                    raise 
                else:
                    sleepMessage = self.moduleName + " -- " + "PullOdataObj " + proc["skip"] +\
                                 " retry attempt " + str(reqAttemptCount) +\
                                 " after sleeping " + self.job["sleepBeforeRetry"]
                    self.logger.debug(sleepMessage) 
                    time.sleep(self.job["sleepBeforeRetry"])
                    continue
                
    def PullOdata(self, proc, fromDate):
        '''
        Sets up the calls so that can start the pulls
        '''
        try:
            self.logger.debug(self.moduleName + " -- " + "PullOdata " + " starting ")

            ###
            #  first get the total number of expected records
            #  the restriction from the ODATA team is that each pull can be only 50 recs
            #
            #  update: 8/2/2017: The ODATA team has upgraded to 1000 recs per pull
            ###
            recordsperpull = self.job["ODATAbatchsize"]
#            recordsperpull = 50
            expectedRecs = int(self.GetRecordCount(proc))
            self.logger.info(self.moduleName + "- Expected Records - " + str(expectedRecs))
            if expectedRecs % recordsperpull == 0:
                numIterations = expectedRecs / recordsperpull
            else:
                numIterations = expectedRecs / recordsperpull + 1
            self.logger.info(self.moduleName + "- Expected number of iterations - " + str(numIterations))
            ####
            #  the next line was only use for testing
            ###
#            numIterations = 100/ recordsperpull
            ###
            urlBase = proc["endpoint"] + proc["name"] + \
                    self.filter + '&$orderby=source_id' + '&$expand=observations' +\
                    '&$top=' + str(self.job["ODATAbatchsize"]) + '&$skip='
            skiprecs = 0
            for ndx in range(0, numIterations):
#                skiprecs = skiprecs + recordsperpull
                skiprecs = ndx * recordsperpull
                if skiprecs > expectedRecs:
                    break
                passparms = {"baseurl": urlBase, "name": proc["name"], "tables": proc["tables"], "skip": str(skiprecs)}
                self.PullOdataObj(passparms, fromDate)
            self.logger.debug(self.moduleName + " -- " + "PullOdata " + " finished ")
        except:
            self.logger.exception(self.moduleName + "- we had an error in PullOdata")
            raise

    def MoveToS3(self, localFolderName, folderName, subFolder):
        '''
        move gzip files to s3 and clean local instance
        localFolderName --> local folder name
        subFolder --> date
        folderName --> folder name on s3
        '''
        try:
            self.logger.debug(self.moduleName + " -- " + "MoveToS3 "  + localFolderName + " starting ")
            ###
            #  move any gzip files to the s3 server
            ###
            s3folder = "s3://" + self.job["bucketName"] + self.job["s3GzipFolderBase"] +\
                        "/" + folderName + '/' + subFolder
            localFolder = self.fileUtilities.gzipFolder + localFolderName
            S3Utilities.SyncFolderAWSCli(localFolder,
                                         s3folder,
                                         args='''--quiet --include "*.gz"''', dbug="Y")
            # Cleanup local files
            FileUtilities.EmptyFolderContents(localFolder)
            self.logger.debug(self.moduleName + " -- " + "MoveToS3 "  + localFolderName + " finished ")
        except:
            self.logger.exception(self.moduleName + " - we had an error in MoveToS3")
            raise

    def LoadData(self, folderName, subFolder, tblJson):
        '''
        load the data from s3 into RedShift
        '''
        try:
            self.logger.debug(self.moduleName + " -- " + "LoadData" + " starting ")
            rsConnect = self.etlUtilities.GetAWSConnection(self.awsParams)

            s3folder = "s3://" + self.job["bucketName"] + self.job["s3GzipFolderBase"] +\
                        "/" + folderName + '/' + subFolder

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
    def CreateUpdateScript(self, pEtlSchema, pEtlTable, tblJson, currProcId):
        '''
        takes the template for the Update sqlscript and customizes it
        '''
        sqlUpdateScript = None
        try:
            self.logger.debug(self.moduleName + " -- " + "UpDate Table Script" + " starting ")
            sqlUpdateTemplate = self.location + '/' + self.job["sqlUpdateScript"]
            sqlUpdateScript = self.localTempDirectory + "/" + re.sub('Template.sql$', '.sql', self.job["sqlUpdateScript"])
            FileUtilities.RemoveFileIfItExists(sqlUpdateScript)
            ###
            #  gather variables needed
            ###

            tbattributesourceName = None
            tbattributedestinationName = None
            tbdatasourceName = None
            tbdatadestinationName = None

            for table in tblJson:
                if "destName" in table:
                    if table["type"] == "attributes":
                        tbattributesourceName = table["table"]
                        tbattributedestinationName = table["destName"]
                    elif table["type"] == "series":
                        tbdatasourceName = table["table"]
                        tbdatadestinationName = table["destName"]

            with open(sqlUpdateTemplate) as infile, open(sqlUpdateScript, 'w') as outfile:
                for line in infile:
                    line = line.replace('{schemaName}', self.job["destinationSchema"])
                    line = line.replace('{tbattributesourceName}', tbattributesourceName)
                    line = line.replace('{tbattributedestinationName}', tbattributedestinationName)
                    line = line.replace('{tbdatasourceName}', tbdatasourceName)
                    line = line.replace('{tbdatadestinationName}', tbdatadestinationName)
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

    def PrepODATACat(self, paramsList, filelocs, currProcId):
        '''
        gets a list of the type of data that we are going to pull and the fields for each
        '''
        try:
            self.logger.debug(self.moduleName + " -- " + "PrepODATACat " + " started ")

            processTableArray = []
            endpoint = self.job["ODATA"]["endpoint"]

            muHelper = MagellanUtilities()
            muHelper.logger = self.logger
            commonParams = {}
            commonParams["sqlFolder"] = self.fileUtilities.sqlFolder
            commonParams["csvFolder"] = self.fileUtilities.csvFolder
            commonParams["gzipFolder"] = self.fileUtilities.gzipFolder

            muHelper.commonParams = commonParams

            for catalog in self.job["ODATA"]["Catalogs"]:
                if catalog["execute"] == "Y":
                    passparms = {"endpoint": endpoint, "name": catalog["name"], "tables": catalog["tables"]}
                    processTableArray.append(passparms)
                    for tblJson in catalog["tables"]:
                        fname = self.fileUtilities.CreateTableSql(tblJson, self.fileUtilities.sqlFolder)
                        RedshiftUtilities.PSqlExecute(fname, self.logger)
                    self.GatherFields(catalog["tables"])
                    self.filter, fromDate = self.CreateFilter(catalog, paramsList)
                    self.logger.debug(fromDate)
                    self.PullOdata(passparms, fromDate)
                    for tblJson in catalog["tables"]:
                        if "destName" in tblJson:
                            self.MoveToS3(tblJson["s3subfolder"][:4], fromDate, tblJson["s3subfolder"])
                            self.LoadData(fromDate, tblJson["s3subfolder"], tblJson)
                    if self.etlUtilities.SetInstanceParameters(filelocs["tblEtl"]["table"],\
                                                               currProcId,\
                                                               json.dumps(
                                                                       {"lastrun":fromDate, "daysback": self.job["daysback"]}
                                                                   )) is not True:
                        self.logger.info(self.moduleName + " - we could not set the instance.")
                    self.UpdateTable(filelocs["tblEtl"]["schemaName"], filelocs["tblEtl"]["table"], catalog["tables"], currProcId)

            self.logger.debug(self.moduleName + " -- " + "PrepODATACat " + " finished ")
        except:
            self.logger.exception(self.moduleName + "- we had an error in PrepODATACat")
            raise

    def Start(self, logger, moduleName, filelocs):
        '''
        Main routine that starts it all
        '''
        currProcId = None
        try:
            ApplicationBase.Start(self, logger, moduleName, filelocs)
            self.logger.debug(self.moduleName + " -- " + "Start " + " started ")
            currProcId = self.etlUtilities.GetRunID(filelocs["tblEtl"]["table"], self.moduleName)
            lastRunRecJson = self.etlUtilities.GetLastGoodRun(filelocs["tblEtl"]["table"], self.moduleName)
            paramsList = []
            if lastRunRecJson["params"] is not None:
                paramsList = json.loads(lastRunRecJson["params"])
###
#  set up to run create folder
###
            self.fileUtilities.moduleName = self.moduleName
            self.fileUtilities.localBaseDirectory = self.localTempDirectory
            self.fileUtilities.CreateFolders(self.job["folders"])
###
            self.PrepODATACat(paramsList, filelocs, currProcId)
            if self.job["cleanlocal"] == "Y":
                self.fileUtilities.RemoveFolder(self.localTempDirectory)

            self.logger.debug(self.moduleName + " -- " + "Start " + " finished ")
        except:
            self.logger.exception(moduleName + " - Exception!")
            if self.etlUtilities.CompleteInstance(filelocs["tblEtl"]["table"],\
                                             currProcId, 'F') is not True:
                self.logger.info(self.moduleName + " - we could not Complete Instance.")
            raise
