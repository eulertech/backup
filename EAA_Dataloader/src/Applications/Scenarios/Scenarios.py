'''
Created on Mar 22, 2017

@author: Hector Hernandez
@summary: Loads the OPEC Calendar data excel file pulled from the OPEC web site.
@note: Restructured to use SQL Server and be pylint compliance: VIU53188
'''

import os
import json
import datetime
import re

from AACloudTools.RedshiftUtilities import RedshiftUtilities
from AACloudTools.FileUtilities import FileUtilities
from AACloudTools.S3Utilities import S3Utilities
from Applications.Common.ApplicationBase import ApplicationBase

class Scenarios(ApplicationBase):
    '''
    This class is used to control the data load process from different OPEC file sources.
    '''

    def __init__(self):
        '''
        Initial settings
        '''
        super(Scenarios, self).__init__()

        self.cBlank = ''
        self.location = FileUtilities.PathToForwardSlash(os.path.dirname(os.path.abspath(__file__)))

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

    def GetMaxUpdateDate(self, tblJson):
        '''
        gets the last known update date so that we can store it in the etl table
        '''
        retVal = None
        try:
            self.logger.debug(self.moduleName + " -- " + "GetMaxUpdateDate" + " starting ")
            sql = '''select count(*) as reccount, max(modifieddate) as lastrun
                        from {}.{}
            '''
            sql = sql.format(self.job["destinationSchema"], tblJson["table"])
            rsConnect = self.etlUtilities.GetAWSConnection(self.awsParams)
            cur = rsConnect.cursor()
            cur.execute(sql)
            tretVal = [dict((cur.description[i][0], value) \
               for i, value in enumerate(row)) for row in cur.fetchall()]
            cur.close()
            lentretVal = len(tretVal)
            if lentretVal > 0:
                retVal = tretVal[0]
            self.logger.debug(self.moduleName + " -- " + "GetMaxUpdateDate" + " finished ")
        except Exception as ex:
            rsConnect.rollback()
            self.logger.exception(self.moduleName + " - we had an error in GetMaxUpdateDate")
            raise ex
        finally:
            cur.close()
            rsConnect.close()
        return retVal

    def BulkUploadToS3(self):
        '''
        Uploads all GZIP files created into S3 to be uploaded later...
        '''
        self.logger.info(self.moduleName + " - Uploading GZIP files to s3 folder...")

        S3Utilities.CopyItemsAWSCli(self.fileUtilities.gzipFolder,
                                    "s3://" + self.job["bucketName"] + self.job["s3GzipFolderBase"] + "/Data",
                                    "--recursive --quiet")

    def CreatePullScript(self, paramsList):
        '''
        takes the template for the pull script and customizes it for the data we need
        '''
        sqlPullDataScript = None
        try:
            self.logger.debug(self.moduleName + " -- " + "CreatePullScript" + " starting ")
            lastDate = None
            fromDate = ''
            lensparamsList = len(paramsList)
            if lensparamsList > 0:
                if "lastrun" in paramsList:
                    lastDate = paramsList["lastrun"]
            if lastDate is not None:
                ###
                #  bump date by one day
                ###
#                fromDate = datetime.datetime.strptime(lastDate, '%m/%d/%Y') +\
#                            datetime.timedelta(days=1)
#                fromDate = fromDate.strftime('%m/%d/%Y')
                fromDate = datetime.datetime.strptime(lastDate, '%m/%d/%Y')
                if fromDate > datetime.datetime.today()-datetime.timedelta(days=1):
                    fromDate = datetime.date.today()-datetime.timedelta(days=1)
                fromDate = datetime.datetime.strftime(fromDate, '%m/%d/%Y')

            sqlPullDataTemplate = self.location + '/sql/' + self.job["sqlPullDataScriptTemplate"]
            sqlPullDataScript = self.localTempDirectory + "/sql/" + re.sub('Template.sql$', '.sql', self.job["sqlPullDataScriptTemplate"])
            FileUtilities.RemoveFileIfItExists(sqlPullDataScript)

            with open(sqlPullDataTemplate) as infile, open(sqlPullDataScript, 'w') as outfile:
                for line in infile:
                    line = line.replace('{lastrundate}', fromDate)
                    outfile.write(line)
            self.logger.debug(self.moduleName + " -- " + "CreatePullScript" + " finished ")
            if fromDate is not self.cBlank:
                fromDate = datetime.datetime.strptime(fromDate, '%m/%d/%Y')
                fromDate = fromDate.strftime('%Y%m%d')
        except:
            self.logger.exception(self.moduleName + " - we had an error in CreatePullScript")
            raise
        return sqlPullDataScript, fromDate

    def BulkExtract(self, sqlPullDataScript, outputCSV):
        '''
        calls BCP module to pull data
        '''
        try:
            self.logger.debug(self.moduleName + " -- " + "BulkExtract" + " starting ")
            self.bcpUtilities.RunBCPJob(self.job["mssqlLoginInfo"],
                                        self.job["bcpUtilityDirOnLinux"],
                                        self.fileUtilities.LoadSQLQuery(sqlPullDataScript),
                                        outputCSV,
                                        self.job["delimiter"],
                                        packetSize='65535')
            self.logger.debug(self.moduleName + " -- " + "BulkExtract" + " finished ")
        except Exception as err:
            self.logger.exception(self.moduleName + " - we had an error in BulkExtract -- " + err.message)
            raise

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
            tbattributedestinationName = None
            tbdatadestinationName = None

            for table in tblJson:
                if "type" in table:
                    if table["type"] == "working":
                        tbworkingsourceName = table["table"]
                    elif table["type"] == "attributes":
                        tbattributedestinationName = table["table"]
                    elif table["type"] == "series":
                        tbdatadestinationName = table["table"]

            with open(sqlUpdateTemplate) as infile, open(sqlUpdateScript, 'w') as outfile:
                for line in infile:
                    line = line.replace('{schemaName}', self.job["destinationSchema"])
                    line = line.replace('{tbworkingsourceName}', tbworkingsourceName)
                    line = line.replace('{tbattributedestinationName}', tbattributedestinationName)
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
    def GetParamsList(self, tblEtl):
        '''
        get the parameter list if one was stored
        '''
        paramsList = []
        try:
            lastRunRecJson = self.etlUtilities.GetLastGoodRun(tblEtl, self.moduleName)
            if lastRunRecJson is not None:
                if lastRunRecJson["params"] is not None:
                    paramsList = json.loads(lastRunRecJson["params"])
        except:
            self.logger.exception(self.moduleName + " - we had an error in GetParamsList")
            raise
        return paramsList

    def Start(self, logger, moduleName, filelocs):
        currProcId = None
        try:
            ApplicationBase.Start(self, logger, moduleName, filelocs)
            self.logger.debug(self.moduleName + " -- " + " starting ")
            currProcId = self.etlUtilities.GetRunID(filelocs["tblEtl"]["table"], self.moduleName)

            paramsList = self.GetParamsList(filelocs["tblEtl"]["table"])
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

            sqlPullDataScript, fromDate = self.CreatePullScript(paramsList)
            outputCSV = self.fileUtilities.csvFolder + fromDate + self.moduleName + ".CSV"
            outputGZ = self.fileUtilities.gzipFolder + fromDate + self.moduleName + '.csv.gz'
            self.BulkExtract(sqlPullDataScript, outputCSV)
            self.fileUtilities.GzipFile(outputCSV, outputGZ)
            self.BulkUploadToS3()
            for tblJson in self.job["tables"]:
                if "s3subfolder" in tblJson:
                    self.LoadData(tblJson["s3subfolder"], tblJson)
                    maxDate = self.GetMaxUpdateDate(tblJson)
                    sMaxDate = maxDate["lastrun"].strftime('%m/%d/%Y')
                    if self.etlUtilities.SetInstanceParameters(filelocs["tblEtl"]["table"],\
                                                               currProcId,\
                                                               json.dumps({"lastrun":sMaxDate})) is not True:
                        self.logger.info(self.moduleName + " - we could not set the instance.")
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
