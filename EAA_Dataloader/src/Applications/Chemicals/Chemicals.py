'''
Created on Feb 5, 2017

@author: VIU53188
@summary: This application will pull Chemical data from sql Server and load it into RedShift
        1)  Creates tables based on configuration
        2)  BCP the data from SQL Server to local CSV file
        3)  create GZ file
        4)  move GZ file to S3
        5)  load into RedShift
        6)  update statistics
'''

import os
import re

from AACloudTools.RedshiftUtilities import RedshiftUtilities
from AACloudTools.FileUtilities import FileUtilities
from AACloudTools.S3Utilities import S3Utilities
from Applications.Common.ApplicationBase import ApplicationBase

class Chemicals(ApplicationBase):
    '''
    classdocs
    '''
    def __init__(self):
        '''
        Constructor
@summary: Explanation of sql
        The primary table is PeriodValues all the other tables are used as supporting
        Product --> to get the definition of the product if needed
        Location --> obtain the description of location and although I am against hardcoding and because of the layout of the data
                1)  LocationTypeID of 2 is regional and this data could be double dipped so we are omitting those as well as
                2)  Exceptions to the rule are USR and DDR that also has the chance to be double dippled
        Category -->  obtain the description of the category ID
                1)  We are only interested in specific categories and based on our Chemical expert they are :
                    a.  10  --> Production
                    b.  12  --> Total Supply
                    c.  15  --> Total Demand
                    d.  19  --> Domestic Demand
        SubCategory -->  although we are not using this table it is important to note it since we are only looking at the summary level
                         where the value = 0 rather than each broken down level from the PeriodValues table

        Last comment is the we are storing the name of the database in the config file since it could change
        periodically depending on what is provided
        for instance currently the last good database is WASP_2015t
        There are two pulls of the database each one 6 months apart.  The first one will be in the form of WASP2015
        and the next on six months later will have a t at the end such as WASP2015t
        '''
        super(Chemicals, self).__init__()
        self.location = FileUtilities.PathToForwardSlash(os.path.dirname(os.path.abspath(__file__)))
        self.s3subFolder = None

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

    def BulkUploadToS3(self):
        '''
        Uploads all GZIP files created into S3 to be uploaded later...
        '''
        self.logger.info(self.moduleName + " - Uploading GZIP files to s3 folder...")
        s3Sub = None
        if self.s3subFolder is not None:
            s3Sub = '/' + self.s3subFolder
        S3Utilities.SyncFolderAWSCli(self.fileUtilities.gzipFolder,
                                     "s3://" + self.job["bucketName"] + self.job["s3GzipFolderBase"] + s3Sub,
                                     args='''--quiet --include "*.gz"''', dbug="Y")

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

    def CreatePullScript(self):
        '''
        takes the template for the pull script and customizes it for the data we need
        '''
        sqlPullDataScript = None
        try:
            self.logger.debug(self.moduleName + " -- " + "CreatePullScript" + " starting ")
            sqlPullDataTemplate = self.location + '/sql/' + self.job["sqlPullDataScriptTemplate"]
            sqlPullDataScript = self.localTempDirectory + "/sql/" + re.sub('Template.sql$', '.sql', self.job["sqlPullDataScriptTemplate"])
            FileUtilities.RemoveFileIfItExists(sqlPullDataScript)

            with open(sqlPullDataTemplate) as infile, open(sqlPullDataScript, 'w') as outfile:
                for line in infile:
                    outfile.write(line)
            self.logger.debug(self.moduleName + " -- " + "CreatePullScript" + " finished ")
        except:
            self.logger.exception(self.moduleName + " - we had an error in CreatePullScript")
            raise
        return sqlPullDataScript

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
        Main routine
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
            sqlPullDataScript = self.CreatePullScript()
            outputCSV = self.fileUtilities.csvFolder + self.moduleName + ".CSV"
            outputGZ = self.fileUtilities.gzipFolder + self.moduleName + '.csv.gz'
            self.BulkExtract(sqlPullDataScript, outputCSV)
            self.fileUtilities.GzipFile(outputCSV, outputGZ)
            self.BulkUploadToS3()
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
        