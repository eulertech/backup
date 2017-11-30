'''
Created on Mar 22, 2017

@author: Hector Hernandez
@summary: Loads the OPEC Calendar data excel file pulled from the OPEC web site.

'''

import os
import shutil

from datetime import datetime
import calendar
import pandas

from AACloudTools.RedshiftUtilities import RedshiftUtilities
from AACloudTools.FileUtilities import FileUtilities
from AACloudTools.S3Utilities import S3Utilities
from Applications.Common.ApplicationBase import ApplicationBase
from xlrd import XLRDError

class Consensus(ApplicationBase):
    '''
    This class is used to control the data load process from different OPEC file sources.
    '''

    def __init__(self):
        '''
        Initial settings
        '''
        super(Consensus, self).__init__()

        self.awsParams = ""
        self.rawFolder = None
        self.csvFolder = None
        self.fileUtilities = FileUtilities(self.logger)
        self.location = FileUtilities.PathToForwardSlash(os.path.dirname(os.path.abspath(__file__)))

    def BulkDownload(self):
        '''
        Download all files.
        '''
        sharedFiles = self.fileUtilities.ScanFolder(self.job["srcSharedFolder"])

        self.logger.info(self.moduleName + " - Downloading files from shared folder...")

        for fileName in sharedFiles:
            if (fileName[:2] == self.job["fileNamePrefix"]) and os.path.splitext(fileName)[1] in self.job["validExts"]:
                shutil.copyfile(os.path.join(self.job["srcSharedFolder"], fileName), self.rawFolder + "/" + fileName)

    def DfCleanUp(self, df, surveyDateVal):
        '''
        Converts the actual excel file into csv for the worksheet configured.
        '''
        bankNameColumnIn = "Unnamed: 0"
        surveyDateColName = "surveyDate"

        for colName in self.job["columnsToDrop"]:
            df = df.drop(colName, 1)

        df = df.drop(self.job["dropAfterHeader"], 0)

        for colName in df.head(0):
            dtTest = colName

            if not isinstance(dtTest, datetime) and colName != bankNameColumnIn:
                df = df.drop(colName, 1)

        df = df.assign(surveyDate=surveyDateVal)

        newOrder = [surveyDateColName]

        for colName in df.head(0):
            if colName != surveyDateColName:
                newOrder.append(colName)

        df = df[newOrder]
        df = df.melt(id_vars=[surveyDateColName, bankNameColumnIn])

        return df

    def LoadAllFromS3(self):
        '''
        Process a single category configured in the categories dictionary in the jobConfig.
        '''
        try:
            s3DataFolder = "s3://" + self.job["bucketName"] + self.job["s3ToDirectory"]

            rsConnect = RedshiftUtilities.Connect(dbname=self.awsParams.redshift['Database'],
                                                  host=self.awsParams.redshift['Hostname'],
                                                  port=self.awsParams.redshift['Port'],
                                                  user=self.awsParams.redshiftCredential['Username'],
                                                  password=self.awsParams.redshiftCredential['Password'])

            RedshiftUtilities.LoadDataFromS3(rsConnect, self.awsParams.s3,
                                             {
                                                 "destinationSchema": self.job["destinationSchema"],
                                                 "tableName": self.job["tableName"],
                                                 "s3Filename": s3DataFolder,
                                                 "fileFormat": self.job["fileFormat"],
                                                 "dateFormat": self.job["dateFormat"],
                                                 "delimiter": self.job["delimiter"]
                                             },
                                             self.logger, "N")

            self.logger.info(self.moduleName + " - Cleaning s3 data folder...")

            S3Utilities.DeleteFileFromS3TempUsingAWSCLi(s3DataFolder, "--recursive --quiet")
        except Exception:
            self.logger.error(self.moduleName + " - Error while trying to save into Redshift from s3 folder.")
            raise

    def GetData(self, rawFileName, mode=None):
        '''
        Returns the data frame or survey date
        '''

        if mode == "getSurveyDate":
            skipRows = 0
        else:
            skipRows = self.job["skipRows"]

        df = pandas.read_excel(rawFileName,
                               sheetname=self.job["worksheetName"],
                               index_col=None,
                               na_values=["na"],
                               skiprows=skipRows,
                               skip_footer=self.job["skipFooter"])

        if mode == "getSurveyDate":
            valRerturn = df.iloc[self.job["surveyDateRow"]-2][0]
        else:
            valRerturn = df

        return valRerturn

    @staticmethod
    def FormatSurveyDate(emFile):
        '''
        Returns the date based on the file's name
        '''

        surveyDateColValue = os.path.splitext(emFile)[0]
        surveyDateColValue = surveyDateColValue[2:len(surveyDateColValue)]
        surveyDateColValue = surveyDateColValue.replace("CF", "")
        surveyDateColValue = str(surveyDateColValue[3:]) + "-" + str(list(calendar.month_abbr).index(surveyDateColValue[:3])) + "-01"

        return surveyDateColValue

    def ProcessFiles(self):
        '''
        Controls the workflow for the conversion, clean up and pack of the input files.
        '''
        filesToProcess = self.fileUtilities.ScanFolder(self.rawFolder)

        for emFile in filesToProcess:
            self.logger.info(self.moduleName + " - Processing file: " + emFile)

            rawFileName = self.rawFolder + "/" + emFile
            csvFilename = self.csvFolder + "/" + os.path.splitext(emFile)[0] + ".csv"

            try:
                surveyDatedt = self.GetData(rawFileName, "getSurveyDate")

                if isinstance(surveyDatedt, float):
                    surveyDatedt = self.FormatSurveyDate(emFile)

                df = self.GetData(rawFileName)
                df = self.DfCleanUp(df, surveyDatedt)

                df.to_csv(csvFilename,
                          header=False,
                          sep=str(self.job["delimiter"]),
                          encoding='utf-8',
                          index=False)

                self.fileUtilities.GzipFile(csvFilename, csvFilename + ".gz")
                self.fileUtilities.DeleteFile(csvFilename)
            except XLRDError:
                self.logger.info(self.moduleName + " - No tab named '" + self.job["worksheetName"] + "' in " + emFile)
            except Exception:
                self.logger.error(self.moduleName + " - Error while trying to process " +  emFile)
                raise
            finally:
                FileUtilities.RemoveFileIfItExists(rawFileName)

    def CheckWorkingFolders(self):
        '''
        Check if the working folders are out there to re-create them
        '''
        self.logger.info(self.moduleName + "Checking on working folders...")

        FileUtilities.RemoveFolder(self.rawFolder)
        FileUtilities.RemoveFolder(self.csvFolder)
        FileUtilities.CreateFolder(self.rawFolder)
        FileUtilities.CreateFolder(self.csvFolder)

    def BulkUploadToS3(self):
        '''
        Uploads all GZIP files created into S3 to be uploaded later...
        '''
        self.logger.info(self.moduleName + " - Uploading GZIP files to s3 folder...")

        S3Utilities.CopyItemsAWSCli(self.csvFolder,
                                    "s3://" + self.job["bucketName"] + self.job["s3ToDirectory"],
                                    "--recursive --quiet")

    def Start(self, logger, moduleName, filelocs):
        currProcId = None
        try:
            ApplicationBase.Start(self, logger, moduleName, filelocs)
            self.logger.debug(self.moduleName + " -- " + " starting ")
            currProcId = self.etlUtilities.GetRunID(filelocs["tblEtl"]["table"], self.moduleName)

            self.rawFolder = self.localTempDirectory + "/" + "Raw"
            self.csvFolder = self.localTempDirectory + "/" + "CSV"

            self.CheckWorkingFolders()
            self.BulkDownload()
            self.ProcessFiles()
            self.BulkUploadToS3()
            self.LoadAllFromS3()
            if self.job["cleanlocal"] == "Y":
                self.fileUtilities.RemoveFolder(self.localTempDirectory)
            self.logger.debug(self.moduleName + " -- " + " finished ")
        except Exception as err:
            self.logger.exception(moduleName + " - Exception! Error: " + err.message)
            if self.etlUtilities.CompleteInstance(filelocs["tblEtl"]["table"],\
                                             currProcId, 'F') is not True:
                self.logger.info(self.moduleName + " - we could not Complete Instance.")
            raise Exception(err.message)
